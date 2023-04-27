import typing as t
from enum import Enum
import copy
from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as exp
from sqlglot import planner

from duckingit._exceptions import WrongInvokationType
from duckingit._parser import Query
from duckingit._utils import split_list_in_chunks, create_hash_string


class Stages(Enum):
    AGGREGATE = "AGGREGATE"
    JOIN = "JOIN"
    SCAN = "SCAN"
    UNION = "UNION"
    SORT = "SORT"


@dataclass
class Task:
    subquery: str
    subquery_hashed: str

    @classmethod
    def create(cls, query: Query, prefixes: list[str]):
        """Creates a step to execute on a serverless function

        Args:
            query, Query: A query parsed by the Query class
            prefixes, list[str]: A list of prefixes to scan

        Returns:
            Task<SUBQUERY | SUBQUERY_HASHED>

        """
        # TODO: Update to use Extension Enum
        subquery = query.copy().sql
        for table in query.tables:
            table = str(table).replace("ARRAY", "LIST_VALUE")  # Current sqlglot bug

            if (
                table[: len("READ_PARQUET")] == "READ_PARQUET"
                or table[: len("SCAN_PARQUET")] == "SCAN_PARQUET"
            ):
                subquery = subquery.replace(table, f"READ_PARQUET({prefixes})")

            for extension in ["JSON", "CSV"]:
                if table[: len(f"READ_{extension}_AUTO")] == f"READ_{extension}_AUTO":
                    subquery = subquery.replace(
                        table, f"READ_{extension}_AUTO({prefixes})"
                    )

        return cls(subquery=subquery, subquery_hashed=create_hash_string(subquery))

    def __repr__(self) -> str:
        return f"Task<QUERY='{self.subquery}' | HASH='{self.subquery_hashed}'>"

    def copy(self):
        """Returns a deep copy of the object itself"""
        return copy.deepcopy(self)


class Stage:
    @classmethod
    def from_ast(
        cls,
        ast: exp.Expression,
        previous_stage: Stage | None = None,
        root_stage: Stage | None = None,
    ):
        ast = ast.copy()
        from_ = ast.args.get("from")
        if isinstance(ast, exp.Select):
            if len(from_.expressions) > 1:
                raise NotImplementedError("Multi FROM is not implemented yet")
            expression = from_.expressions[0]

            if isinstance(expression, exp.Subquery):
                stage = cls.select_stage_type(ast)
                stage.name = expression.sql()
                stage.sql = ast.sql()
                stage.ast = ast
                if previous_stage is not None:
                    previous_stage.add_dependency(stage)
                if root_stage is None:
                    root_stage = stage
                stage = Stage.from_ast(
                    expression.this, previous_stage=stage, root_stage=root_stage
                )

            elif isinstance(expression, exp.Union):
                raise NotImplementedError("Cannot handle Unions yet")

            else:
                stage = cls.select_stage_type(ast)
                stage.name = expression.sql()
                stage.sql = ast.sql()
                stage.ast = ast
                if previous_stage is not None:
                    previous_stage.add_dependency(stage)

                if root_stage is None:
                    root_stage = stage
        else:
            raise NotImplementedError()

        if root_stage is None:
            root_stage = stage
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

    def __init__(self):
        self.name: str = ""
        self.sql: str = ""
        self.ast: exp.Expression | None = None
        self.dependents = []
        self.dependencies = []

    def __repr__(self) -> str:
        return f"{self.name}: {self.sql}"

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

    def __init__(self, query: Query, stages: list[Stage]) -> None:
        self.query = query
        self.stages = stages

    def __len__(self) -> int:
        return len(self.stages)

    @classmethod
    def from_query(cls, query: Query):
        root = query.ast.copy()
        try:
            root.find(exp.With).pop()
        except AttributeError():
            pass

        ast = query.ast.copy()

        def replace_from(
            expr: exp.Expression, table_name: str, alias: str = ""
        ) -> None:
            stmt = exp.From(
                expressions=[
                    exp.Table(
                        this=exp.Identifier(this=table_name, quoted=False),
                        alias=exp.TableAlias(
                            this=exp.Identifier(this=alias, quoted=False)
                        ),
                    )
                ]
            )

            expr.find(exp.From).replace(stmt)

    # @classmethod
    # def create_from_query(cls, query: Query, invokations: int | str):
    #     if isinstance(invokations, str):
    #         if invokations != "auto":
    #             raise WrongInvokationType(
    #                 "`invokations` can only be 'auto' or an integer"
    #             )
    #         invokations = len(query.list_of_prefixes)

    #     # TODO: Heuristic to divide the workload between the invokations based on size
    #     # of prefixes / number of files etc. Or based on some deeper analysis of the query?
    #     chunks_of_prefixes = split_list_in_chunks(
    #         query.list_of_prefixes, number_of_invokations=invokations
    #     )

    #     execution_steps: list[Task] = []
    #     for chunk in chunks_of_prefixes:
    #         execution_steps.append(Task.create(query=query, prefixes=chunk))

    #     return cls(query=query, execution_steps=execution_steps)

    def __repr__(self) -> str:
        return f"{self.stages}"

    def copy(self):
        """Returns a deep copy of the object itself"""
        return copy.deepcopy(self)
