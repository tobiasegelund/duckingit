import typing as t
from enum import Enum
import copy
from dataclasses import dataclass

import sqlglot.expressions as exp

from duckingit._parser import Query
from duckingit._utils import split_list_in_chunks, create_hash_string


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
    stage_type = ""

    @classmethod
    def from_ast(
        cls,
        ast: exp.Expression,
        previous_stage: None = None,
        root_stage: None = None,
        cte_stages: dict = {},
    ):
        ast = ast.copy()

        with_ = ast.args.get("with")
        try:
            ast.find(exp.With).pop()
        except AttributeError:
            pass

        if with_:
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
        self.id = str = ""
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
    def name_or_sql(self) -> None:
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
        invokations = (
            DuckConfig().session.max_invokations
            if self.stage_type == Stages.SCAN
            else 1
        )

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
    if group:
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

    The execution plan consists of a execution steps based on queries. Basically, the
    class scan the bucket based on the query, divides the workload on the number of
    invokations. Afterwards, its the Controller's job to execute the plan.

    Attributes:

    Methods:
        from_query: Creates an execution plan that divides the workload between
            nodes
    """

    def __init__(
        self, query: Query, root: Stage, dag: dict[Stage, t.Set[Stage]]
    ) -> None:
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

        dag = {}
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
