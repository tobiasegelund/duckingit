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
    SET = "SET"
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
    dependents = set()
    dependencies = set()

    @classmethod
    def from_ast(cls, ast: exp.Expression, context: dict = {}):
        ast = ast.copy()

        with_ = ast.args.get("with")
        for cte in with_:
            pass

        from_ = ast.args.get("from")
        if isinstance(ast, exp.Select):
            from_ = from_.expressions[0]

            if isinstance(from_, exp.Subquery):
                name = from_.alias_or_name
                stage = Stage.from_ast(from_.this)

            stage = Scan.from_ast(ast)

        # group = ast.args.get("group")

        # if group:
        #     stage = Aggregate.from_ast(ast)

        # order = ast.args.get("order")

        # if order:
        #     stage = Sort.from_ast(ast)

    def __init__(self, name: str = "", query: str = ""):
        # Name of the table
        self.name = name
        # The query to run in the stage
        self.query = query

    def add_depedency(self, dependency: Stage):
        self.dependencies.add(dependency)
        dependency.dependents.add(self)

    def copy(self):
        """Returns a deep copy of the object itself"""
        return copy.deepcopy(self)


class Scan(Stage):
    stage_type = Stages.SCAN

    @classmethod
    def from_ast(cls, ast: exp.Expression):
        pass


class Aggregate(Stage):
    stage_type = Stages.AGGREGATE

    @classmethod
    def from_ast(cls, ast: exp.Expression):
        pass


class Sort(Stage):
    stage_type = Stages.SORT

    @classmethod
    def from_ast(cls, ast: exp.Expression):
        pass


class Join(Stage):
    stage_type = Stages.JOIN

    @classmethod
    def from_ast(cls, ast: exp.Expression):
        pass


class SetOperation(Stage):
    stage_type = Stages.SET

    @classmethod
    def from_ast(cls, ast: exp.Expression):
        pass


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
        # Table name => hash values to locate
        context: dict[str, Stage] = {}

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
