import copy
import typing as t
from dataclasses import dataclass
from enum import Enum

import sqlglot.expressions as exp

from duckingit._parser import Query
from duckingit._utils import create_hash_string, split_list_in_chunks


class Stages(Enum):
    AGGREGATE = "AGGREGATE"
    CTE = "CTE"
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
    def create(cls, query: Query, files: list[str]):
        """Creates a task to execute on a serverless function

        Args:
            query, Query: A query parsed by the Query class
            files, list[str]: A list of files to scan

        Returns:
            Task<SUBQUERY | SUBQUERY_HASHED>

        """
        subquery = query.copy().sql
        for table in query.from_:
            table = table.expressions[0]
            alias = table.alias
            table = str(table).replace("ARRAY", "LIST_VALUE")  # Current sqlglot bug

            read_json = "READ_JSON_AUTO"
            read_csv = "READ_CSV_AUTO"

            if table[: len(read_json)] == read_json:
                subquery = subquery.replace(table, f"READ_JSON_AUTO({files}) {alias}")
            elif table[: len(read_csv)] == read_csv:
                subquery = subquery.replace(table, f"READ_CSV_AUTO({files}) {alias}")
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
        previous_stage: "Stage" | None = None,
        root_stage: "Stage" | None = None,
        cte_stages: dict = {},
    ):
        ast = ast.copy()

        with_ = ast.args.get("with")

        if with_:
            ast.find(exp.With).pop()  # type: ignore

            cte_stages = cte_stages.copy()
            for cte in with_.expressions:
                stage = select_stage_type(cte)
                stage.id = create_hash_string(cte.sql(), digits=6)
                stage.name = cte.alias
                stage.alias = cte.alias
                stage.from_ = cte.this.sql()
                stage.sql = cte.sql()

                stage = Stage.from_ast(
                    cte.this,
                    previous_stage=stage,
                    root_stage=root_stage,
                    cte_stages=cte_stages,
                )

                cte_stages[cte.alias] = stage

        from_ = ast.args.get("from")
        if isinstance(ast, exp.Select) and from_:
            if len(from_.expressions) > 1:
                raise NotImplementedError("Multi FROM isn't supported")
            expression = from_.expressions[0]

            if isinstance(expression, exp.Subquery):
                stage = select_stage_type(ast)
                stage.id = create_hash_string(ast.sql(), digits=6)
                stage.from_ = expression.sql()
                stage.alias = expression.alias
                stage.sql = ast.sql()

                if previous_stage is not None:
                    previous_stage.add_dependency(stage)
                if root_stage is None:
                    root_stage = stage

                assert expression.this

                stage = Stage.from_ast(
                    expression.this,
                    previous_stage=stage,
                    root_stage=root_stage,
                    cte_stages=cte_stages,
                )

            elif isinstance(expression, exp.Union):
                raise NotImplementedError("Cannot handle Unions yet")

            else:
                table_name = expression.sql()

                stage = select_stage_type(ast)
                stage.id = create_hash_string(ast.sql(), digits=6)
                stage.alias = expression.alias
                stage.from_ = table_name
                stage.sql = ast.sql()

                if table_name in cte_stages:
                    cte = cte_stages[table_name]
                    stage.add_dependency(cte)

                if previous_stage is not None:
                    previous_stage.add_dependency(stage)

                if root_stage is None:
                    root_stage = stage
        else:
            raise NotImplementedError()

        return root_stage

    def __init__(self):
        self.id: str = ""
        self.name: str = ""
        self.alias: str = ""  # Properbly to be deleted
        self.sql: str = ""
        self.from_: str = ""

        self.dependents = []
        self.dependencies = []

        self.tasks: t.Set[Task] = set()

    def __repr__(self) -> str:
        return f"{self.stage_type} - {self.id}: {self.sql}"

    def __len__(self) -> int:
        return len(self.tasks)

    @property
    def name_or_sql(self) -> str:
        if self.name == "":
            return self.sql
        return self.name

    @property
    def output(self) -> list[str]:
        return list(task.subquery_hashed for task in self.tasks)

    def create_tasks(self, dependencies: dict[str, list[str]] = {}) -> None:
        # TODO: Focus on Stage ID in dependencies
        from duckingit._config import DuckConfig

        query = Query.parse(self.sql)
        if dependencies:
            files = dependencies["output"]
        else:
            files = query.list_of_prefixes

        # Wide operations can only have 1 invokation
        # Narrow operations like SCAN can have multiple invokations
        invokations = DuckConfig().session.max_invokations if self.stage_type == Stages.SCAN else 1

        if isinstance(invokations, str):
            invokations = len(files)

        # TODO: Heuristic to divide the workload between the invokations based on size
        # of prefixes / number of files etc. Or based on some deeper analysis of the query?
        chunks_of_files = split_list_in_chunks(files, number_of_invokations=invokations)

        for chunk in chunks_of_files:
            self.tasks.add(Task.create(query=query, files=chunk))

    def add_dependency(self, dependency: "Stage"):
        self.dependencies.append(dependency)
        dependency.dependents.append(self)

    def copy(self):
        """Returns a deep copy of the object itself"""
        return copy.deepcopy(self)


class Scan(Stage):
    stage_type = Stages.SCAN

    def __init__(self):
        super().__init__()


class CTE(Stage):
    stage_type = Stages.CTE

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
    if isinstance(ast, exp.CTE):
        return CTE()

    group = ast.args.get("group")
    agg = list(i for i in ast.expressions if isinstance(i, exp.AggFunc))
    if group or agg:
        return Aggregate()

    sort = ast.args.get("order")
    if sort:
        return Sort()

    join = ast.args.get("join")
    if join:
        raise NotImplementedError("Joins are not implemented yet")

    return Scan()


class Plan:
    """Class to create an execution plan across nodes

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

    @property
    def leaves(self) -> list[Stage]:
        return [node for node, deps in self.dag.items() if not deps]

    @classmethod
    def from_query(cls, query: Query):
        root = Stage.from_ast(ast=query.ast)

        dag: dict[Stage, t.Set[Stage]] = {}
        nodes = {root}
        while nodes:
            node = nodes.pop()

            assert node

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
