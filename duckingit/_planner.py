import copy
import typing as t
from dataclasses import dataclass
from enum import Enum

import sqlglot
import sqlglot.expressions as exp

from duckingit._parser import Query
from duckingit._utils import create_hash_string, split_list_in_chunks


class Stages(Enum):
    AGGREGATE = "AGGREGATE"
    JOIN = "JOIN"
    SCAN = "SCAN"
    UNION = "UNION"
    SORT = "SORT"

    def __str__(self) -> str:
        return f"{self.value}"


@dataclass
class Task:
    subquery: str
    subquery_hashed: str

    @classmethod
    def create(cls, query: Query, files: list[str] | None = None):
        """Creates a task to execute on a serverless function

        Args:
            query, Query: A query parsed by the Query class
            files, list[str]: A list of files to scan

        Returns:
            Task<SUBQUERY | SUBQUERY_HASHED>

        """
        subquery = query.copy().sql

        if files:
            for table in query.from_:
                table = table.expressions[0]
                alias = table.alias
                table = str(table).replace("ARRAY", "LIST_VALUE")  # Current sqlglot bug

                read_json = "READ_JSON_AUTO"
                read_csv = "READ_CSV_AUTO"

                if table[: len(read_json)] == read_json:
                    subquery = subquery.replace(table, f"{read_json}({files}) {alias}")
                elif table[: len(read_csv)] == read_csv:
                    subquery = subquery.replace(table, f"{read_csv}({files}) {alias}")
                else:
                    subquery = subquery.replace(table, f"READ_PARQUET({files}) {alias}")

        return cls(subquery=subquery, subquery_hashed=create_hash_string(subquery))

    def __hash__(self) -> int:
        return hash(self.subquery)

    def __repr__(self) -> str:
        return f"Task<QUERY='{self.subquery}' | HASH='{self.subquery_hashed}'>"

    def copy(self):
        """Returns a deep copy of the object itself"""
        return copy.deepcopy(self)


class Stage:
    stage_type: Stages

    @classmethod
    def from_ast(
        cls,
        ast: exp.Expression,
        previous_stage: t.Optional["Stage"] = None,
        cte_stages: dict = {},
    ):
        ast = ast.copy()

        with_ = ast.args.get("with")
        if with_:
            ast.find(exp.With).pop()  # type: ignore

            cte_stages = cte_stages.copy()
            for cte in with_.expressions:
                stage = Stage.from_ast(
                    cte.this,
                    previous_stage=previous_stage,
                    cte_stages=cte_stages,
                )
                stage.alias = cte.alias

                cte_stages[cte.alias] = stage

        from_ = ast.args.get("from")
        if isinstance(ast, exp.Select) and from_:
            if len(from_.expressions) > 1:
                raise NotImplementedError("Multi FROM isn't supported")
            expression = from_.expressions[0]
            assert expression.this

            if isinstance(expression, exp.Subquery):
                stage = select_stage_type(ast)
                # id must begin with a character
                stage.id = create_hash_string(ast.sql(), digits=6, first_char="$")
                stage.alias = expression.alias
                stage.ast = ast.copy()  # type: ignore

                if previous_stage is not None:
                    previous_stage.add_dependency(stage)

                sub_stage = Stage.from_ast(
                    expression.this,
                    previous_stage=stage,
                    cte_stages=cte_stages,
                )
                stage.replace_child_with_id(
                    child=expression, id=sub_stage.id, alias=expression.alias
                )

            elif isinstance(expression, exp.Union):
                raise NotImplementedError("Cannot handle Unions yet")

            else:
                table_name = str(expression.this)

                stage = select_stage_type(ast)
                stage.id = create_hash_string(ast.sql(), digits=6, first_char="$")
                stage.alias = expression.alias
                stage.ast = ast.copy()  # type: ignore

                if table_name in cte_stages:
                    cte = cte_stages[table_name]
                    stage.replace_child_with_id(child=expression, id=cte.id, alias=expression.alias)
                    stage.add_dependency(cte)

        else:
            stage = Scan()

        joins = ast.args.get("joins")
        if joins:
            stage.stage_type = Stages.JOIN

            for join in joins:
                join = join.this
                alias = join.alias

                if isinstance(join, exp.Subquery):
                    subquery_stage = Stage.from_ast(
                        join.this,  # type: ignore
                        previous_stage=previous_stage,
                        cte_stages=cte_stages,
                    )
                    stage.replace_child_with_id(child=join, id=subquery_stage.id, alias=alias)
                    stage.add_dependency(subquery_stage)

                else:
                    if (table_name := join.this.sql()) in cte_stages:
                        cte = cte_stages[table_name]
                        stage.replace_child_with_id(child=join, id=cte.id, alias=alias)
                        stage.add_dependency(cte)

        if previous_stage is not None:
            previous_stage.add_dependency(stage)

        return stage

    def __init__(self):
        self.id: str = ""
        self.name: str = ""
        self.alias: str = ""  # Properbly to be deleted
        self.ast: exp.Expression | None = None
        self._sql: str = ""

        self.dependents: t.Set["Stage"] = set()
        self.dependencies: t.Set["Stage"] = set()

        self.tasks: t.Set[Task] = set()

    def __repr__(self) -> str:
        return f"{self.stage_type} - {self.id}: {self.sql}"

    def __len__(self) -> int:
        return len(self.tasks)

    def replace_child_with_id(self, child: exp.Expression, id: str, alias: str = "") -> None:
        if self.ast is None:
            raise ValueError

        for node, *_ in self.ast.walk():
            if node == child:
                node.replace(sqlglot.parse_one(f"{id} {alias}"))

    @property
    def sql(self) -> str:
        if self.ast is None:
            return ""
        return self.ast.sql()

    def alias_or_id(self) -> str:
        if self.alias == "":
            return self.id
        return self.alias

    @property
    def output(self) -> list[str]:
        return list(task.subquery_hashed for task in self.tasks)

    def create_tasks(self, dependencies: dict[str, list[str]] = {}) -> None:
        # TODO: Focus on Stage ID in dependencies
        from duckingit._config import DuckConfig

        # Wide operations can only have 1 invokation
        # Narrow operations like SCAN can have multiple invokations
        invokations = DuckConfig().session.max_invokations if self.stage_type == Stages.SCAN else 1

        query = Query.parse(self.sql)
        if dependencies:
            for _id, output in dependencies.items():
                query.replace(_id, f"(SELECT * FROM READ_PARQUET({output}))")

            self.tasks.add(Task.create(query=query))

        else:
            files = query.list_of_prefixes

            if isinstance(invokations, str):
                invokations = len(files)

            # TODO: Heuristic to divide the workload between the invokations based on size
            # of prefixes / number of files etc. Or based on some deeper analysis of the query?
            chunks_of_files = split_list_in_chunks(files, number_of_invokations=invokations)

            for chunk in chunks_of_files:
                self.tasks.add(Task.create(query=query, files=chunk))

    def add_dependency(self, dependency: "Stage") -> None:
        self.dependencies.add(dependency)
        dependency.dependents.add(self)

    def copy(self):
        """Returns a deep copy of itself"""
        return copy.deepcopy(self)


class Scan(Stage):
    stage_type = Stages.SCAN

    def __init__(self):
        super().__init__()


class Aggregate(Stage):
    stage_type = Stages.AGGREGATE

    def __init__(self):
        super().__init__()


class Sort(Stage):
    stage_type = Stages.SORT

    def __init__(self):
        super().__init__()


class Join(Stage):
    stage_type = Stages.JOIN

    def __init__(self):
        super().__init__()


class Union(Stage):
    stage_type = Stages.UNION

    def __init__(self):
        super().__init__()


def select_stage_type(ast: exp.Expression):
    group = ast.args.get("group")
    agg = list(i for i in ast.expressions if isinstance(i, exp.AggFunc))
    if group or agg:
        return Aggregate()

    sort = ast.args.get("order")
    if sort:
        return Sort()

    return Scan()


class Plan:
    """The execution plan

    A execution consists of stages, and each stage consists of tasks. A stage is an actual
    operation, such as Scan, Sort, Join, where a task is the actual DuckDB SQL query to run on
    a serverless function

    Attributes:
        query, Query: The main query parsed as in the Query class
        root, Stage: The root operation, ie. last operation, in the DAG
        dag, dict[Stage, Set(Stage)]: A DAG that represents the execution plan in nodes
        leaves, list[Stage]: The leaves of stages in the DAG

    Methods:
        from_query: Creates an execution plan from a query parsed in the Query class
    """

    def __init__(self, query: Query, root: Stage, dag: dict[Stage, t.Set[Stage]]) -> None:
        self.query = query
        self.root = root
        self.dag = dag

        self._length: int | None = None

    def __len__(self) -> int:
        if self._length is None:
            self._length = len([node for node, _ in self.dag.items()])
        return self._length

    @classmethod
    def from_query(cls, query: Query):
        root = Stage.from_ast(ast=query.ast)

        dag: dict[Stage, t.Set[Stage]] = {root: set()}
        nodes = {root}
        while nodes:
            node = nodes.pop()

            dag[node] = set()
            for dep in node.dependencies:
                dag[node].add(dep)
                nodes.add(dep)

        return cls(query=query, root=root, dag=dag)

    def __repr__(self) -> str:
        return f"{self.dag}"

    def copy(self):
        """Returns a deep copy of the object itself"""
        return copy.deepcopy(self)
