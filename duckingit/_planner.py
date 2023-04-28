import re
import typing as t
from enum import Enum
import copy
from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as exp
from sqlglot import planner

from duckingit._parser import Query
from duckingit._utils import split_list_in_chunks, create_hash_string, flatten_list


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
    def create(cls, query: Query, prefixes: list[str]):
        """Creates a task to execute on a serverless function

        Args:
            query, Query: A query parsed by the Query class
            prefixes, list[str]: A list of prefixes to scan

        Returns:
            Task<SUBQUERY | SUBQUERY_HASHED>

        """
        # TODO: Update to use Extension Enum
        # TODO: How to handle alias?
        subquery = query.copy().sql
        for table in query.tables:
            table = str(table).replace("ARRAY", "LIST_VALUE")  # Current sqlglot bug

            if table[: len("READ_JSON_AUTO")] == "READ_JSON_AUTO":
                subquery = subquery.replace(table, f"READ_JSON_AUTO({prefixes})")
            elif table[: len("READ_CSV_AUTO")] == "READ_CSV_AUTO":
                subquery = subquery.replace(table, f"READ_CSV_AUTO({prefixes})")
            else:
                subquery = subquery.replace(table, f"READ_PARQUET({prefixes})")

        return cls(subquery=subquery, subquery_hashed=create_hash_string(subquery))

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
                stage = cls.select_stage_type(cte)
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
        if isinstance(ast, exp.Select):
            if len(from_.expressions) > 1:
                raise NotImplementedError("Multi FROM is not implemented yet")
            expression = from_.expressions[0]

            if isinstance(expression, exp.Subquery):
                stage = cls.select_stage_type(ast)
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

                stage = cls.select_stage_type(ast)
                stage.id = create_hash_string(ast.sql(), digits=6)
                stage.alias = expression.alias
                stage.from_ = table_name
                stage.sql = ast.sql()

                if table_name in cte_stages:
                    cte = cte_stages[table_name]
                    stage.add_dependency(cte)
                    # stage.from_ = cte.sql

                if previous_stage is not None:
                    previous_stage.add_dependency(stage)

                if root_stage is None:
                    root_stage = stage
        else:
            raise NotImplementedError()

        return root_stage

        @classmethod
        def select_stage_type(cls, ast: exp.Expression):
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

    @classmethod
    def select_stage_type(cls, ast: exp.Expression):
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

    def __init__(self):
        self.id = str = ""
        self.name: str = ""
        self.alias: str = ""  # Properbly to be deleted
        self.sql: str = ""
        self.from_: str = ""

        self.dependents = []
        self.dependencies = []

        self.tasks: list[Task] = []

    def __repr__(self) -> str:
        return f"{self.stage_type} - {self.id}: {self.sql}"

    @property
    def name_or_sql(self) -> None:
        if self.name == "":
            return self.sql
        return self.name

    @property
    def output(self) -> list[str]:
        return list(task.subquery_hashed for task in self.tasks)

    def create_tasks(self, dependency: dict[str : list[str]] | None = None) -> None:
        # Dependency will change to multiple dependencies in the future
        from duckingit._config import DuckConfig, CACHE_PREFIX

        query = Query.parse(self.sql)
        if dependency is None:
            prefixes = query.list_of_prefixes
        else:
            prefixes = list(v for _, v in dependency.items())

        # Wide operations can only have 1 invokation
        # Narrow operations like SCAN can have multiple invokations
        invokations = (
            DuckConfig().session.max_invokations
            if self.stage_type == Stages.SCAN
            else 1
        )

        if isinstance(invokations, str):
            invokations = len(prefixes)

        # TODO: Heuristic to divide the workload between the invokations based on size
        # of prefixes / number of files etc. Or based on some deeper analysis of the query?
        chunks_of_prefixes = split_list_in_chunks(
            prefixes, number_of_invokations=invokations
        )

        _tasks: list[Task] = []
        for chunk in chunks_of_prefixes:
            _tasks.append(Task.create(query=query, prefixes=chunk))

        self.tasks = _tasks

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


class Plan:
    """Class to create an execution plan across nodes

    The execution plan consists of a execution steps based on queries. Basically, the
    class scan the bucket based on the query, divides the workload on the number of
    invokations. Afterwards, its the Controller's job to execute the plan.

    Attributes:
        query, Query: A query parsed by the Query class
        execution_steps, list[step]: A list of steps to execute using the serverless
            function

    Methods:
        create_from_query: Creates an execution plan that divides the workload between
            nodes
    """

    def __init__(
        self, query: Query, root: Stage, dag: dict[Stage, t.Set[Stage]]
    ) -> None:
        self.query = query
        self.root = root
        self.dag = dag

        # self.stages = stages

    def __len__(self) -> int:
        return len(self.stages)

    def leaves(self) -> list[Stage]:
        return [node for node, deps in self.dag.items() if not deps]

    @classmethod
    def from_query(cls, query: Query):
        root = Stage.from_ast(ast=query.ast)
        bucket = query.bucket
        context: dict[str, list[str]] = {}

        # TODO:
        # Replace FROM statements and secure alias
        # Focus on Stage ID
        # Create tasks within stages

        dag = {}
        nodes = {root}
        while nodes:
            node = nodes.pop()

            dag[node] = set()
            for dep in node.dependencies:
                dag[node].add(dep)
                nodes.add(dep)

        queue = set(node for node, deps in dag.items() if not deps)
        while queue:
            node = queue.pop()

            for deb in node.dependents:
                if deb.stage_type == Stages.CTE:
                    continue
                queue.add(deb)

        return cls(query=query, root=root, dag=dag)

    def __repr__(self) -> str:
        return f"{self.stages}"

    def copy(self):
        """Returns a deep copy of the object itself"""
        return copy.deepcopy(self)
