import typing as t
from enum import Enum
import copy
from dataclasses import dataclass

import sqlglot
from sqlglot import expressions
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
    def __init__(
        self,
        stage_type: Stages,
        stage_query: str,
        dependencies: list[str] | None = None,
    ):
        self.stage_type = stage_type
        self.stage_query = stage_query

        self.dependencies = dependencies

        # Create execution tasks here
        # self.execution_tasks: list[Task] = execution_tasks

    def __repr__(self):
        return f"{self.stage_type.value}<{self.stage_query}>"

    @classmethod
    def from_step(cls, node: planner.Step):
        if isinstance(node, planner.Scan):
            stage_type, operations = cls.scan(node=node)
        elif isinstance(node, planner.Aggregate):
            stage_type, operations = cls.aggregate(node=node)
        elif isinstance(node, planner.Join):
            stage_type, operations = cls.join(node=node)
        elif isinstance(node, planner.SetOperation):
            stage_type, operations = cls.set_operation(node=node)
        elif isinstance(node, planner.sort):
            stage_type, operations = cls.sort(node=node)
        else:
            raise NotImplementedError()

        query = " ".join(operations)

        return cls(stage_type=stage_type, stage_query=query)

    @classmethod
    def scan(cls, node: planner.Scan) -> tuple[Stages, list[str]]:
        operations = []

        if expr := node.projections:
            operations += cls.select(expr)
        else:
            for dep in node.dependents:
                operations += cls.select(dep.projections)

        operations += cls.from_(node.source)

        if expr := node.condition:
            operations += cls.where(expr)

        if (expr := node.limit) and isinstance(node.limit, int):
            operations += cls.limit(expr)

        return Stages.SCAN, operations

    @classmethod
    def aggregate(cls, node: planner.Aggregate) -> tuple[Stages, list[str]]:
        operations = []

        if node.group:
            operations += cls.select(node.group)
            operations += cls.aggregations(node.aggregations)
            operations += cls.from_(node.source)
            operations += cls.group_by(node.group)
            if expr := node.condition:
                operations += cls.where(expr)

            if (expr := node.limit) and isinstance(node.limit, int):
                operations += cls.limit(expr)

        else:
            raise ValueError("Missing GROUP BY statement in query")

        return Stages.AGGREGATE, operations

    @classmethod
    def sort(cls, node: planner.Sort) -> tuple[Stages, list[str]]:
        raise NotImplementedError()

    @classmethod
    def set_operation(cls, node: planner.SetOperation) -> tuple[Stages, list[str]]:
        raise NotImplementedError()

    @classmethod
    def join(cls, node: planner.Join) -> tuple[Stages, list[str]]:
        raise NotImplementedError()

    @classmethod
    def select(
        cls, exprs: list[expressions.Column] | dict[str, expressions.Column]
    ) -> list[str]:
        stmt = ["SELECT"]
        if isinstance(exprs, list):
            stmt.append(", ".join(col.sql() for col in exprs))
        if isinstance(exprs, dict):
            stmt.append(" ".join(col.sql() + "," for col in exprs.values()))
        return stmt

    @classmethod
    def group_by(cls, exprs: dict[str, expressions.Column]) -> list[str]:
        return ["GROUP BY", ", ".join(col.sql() for col in exprs.values())]

    @classmethod
    def from_(cls, expr: expressions.Table | str) -> list[str]:
        stmt = ["FROM"]
        if isinstance(expr, str):
            stmt.append(expr)
        else:
            stmt.append(expr.sql())
        return stmt

    @classmethod
    def where(cls, expr: expressions.Where) -> list[str]:
        return ["WHERE", expr.sql()]

    @classmethod
    def limit(cls, expr: expressions.Limit) -> list[str]:
        return ["LIMIT", str(expr)]

    @classmethod
    def aggregations(cls, exprs: list[expressions.AggFunc]) -> list[str]:
        return [", ".join(col.sql() for col in exprs)]

    @classmethod
    def order_by(cls, expr: list[expressions.Ordered]) -> list[str]:
        return ["ORDER BY", ", ".join(col.sql() for col in expr)]

    def copy(self):
        """Returns a deep copy of the object itself"""
        return copy.deepcopy(self)


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
    def from_query(cls, query: Query) -> None:
        plan = planner.Plan(query.ast)

        stages = []
        completed = set()
        queue = set(plan.leaves)

        while queue:
            node = queue.pop()

            stage = Stage.from_step(node)

            completed.add(node)
            stages.append(stage)

            for deb in node.dependents:
                if deb not in completed:
                    queue.add(deb)

        # root = plan.root
        return cls(query=query, stages=stages)

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
