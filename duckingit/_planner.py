import typing as t
from enum import Enum
import copy
from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as expr
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
    def from_step(cls, node: planner.Step, context: list[str] | None = None):
        if isinstance(node, planner.Scan):
            stage_type, operations = cls.scan(node=node, context=context)
        elif isinstance(node, planner.Aggregate):
            stage_type, operations = cls.aggregate(node=node, context=context)
        elif isinstance(node, planner.Join):
            stage_type, operations = cls.join(node=node, context=context)
        elif isinstance(node, planner.SetOperation):
            stage_type, operations = cls.set_operation(node=node, context=context)
        elif isinstance(node, planner.Sort):
            stage_type, operations = cls.sort(node=node, context=context)
        else:
            raise NotImplementedError()

        query = " ".join(operations)

        return cls(stage_type=stage_type, stage_query=query)

    @classmethod
    def scan(
        cls, node: planner.Scan, context: list[str] | None = None
    ) -> tuple[Stages, list[str]]:
        operations = []

        projections = node.projections
        if projections:
            operations += cls.select(projections)
        else:
            operations.append("SELECT")
            for dep in node.dependents:
                if isinstance(dep, planner.Aggregate):
                    operations.append(
                        ", ".join(
                            list(col.name for col in dep.group.values())
                            + list(
                                col.find(expr.Column).name for col in dep.aggregations
                            )
                        )
                    )
                else:
                    # TODO: Discover which other options apply
                    raise ValueError("Planner.Aggregate doesn't only apply anymore")

        from_ = node.source
        # TODO consider to use from_ = "" to apply dependency => Apply context
        if from_ is None:
            raise ValueError("FROM is None")
        else:
            operations += cls.from_(from_)

        condition = node.condition
        if condition:
            operations += cls.where(condition)

        limit = node.limit
        if isinstance(limit, int):
            operations += cls.limit(limit)

        return Stages.SCAN, operations

    @classmethod
    def aggregate(
        cls, node: planner.Aggregate, context: list[str] | None = None
    ) -> tuple[Stages, list[str]]:
        operations = []

        if node.group:
            operations += cls.select(node.group)
            operations += cls.aggregations(node.aggregations)

            from_ = node.source
            if from_ is None:
                raise ValueError("FROM is None")
            else:
                operations += cls.from_(from_)

            condition = node.condition
            if condition:
                operations += cls.where(condition)

            operations += cls.group_by(node.group)

            limit = node.limit
            if isinstance(limit, int):
                operations += cls.limit(limit)

        else:
            raise ValueError("Missing GROUP BY statement in query")

        return Stages.AGGREGATE, operations

    @classmethod
    def sort(
        cls, node: planner.Sort, context: list[str] | None = None
    ) -> tuple[Stages, list[str]]:
        operations = []

        projections = node.projections
        if projections:
            operations += cls.select(node.projections)

            # TODO: Apply context!!
            # from_ = node.source

            key = node.key
            if key:
                operations += cls.order_by(key)

        return Stages.SORT, operations

    @classmethod
    def set_operation(
        cls, node: planner.SetOperation, context: list[str] | None = None
    ) -> tuple[Stages, list[str]]:
        raise NotImplementedError()

    @classmethod
    def join(
        cls, node: planner.Join, context: list[str] | None = None
    ) -> tuple[Stages, list[str]]:
        raise NotImplementedError()

    @classmethod
    def select(
        cls, exp: t.Sequence[expr.Expression] | dict[str, expr.Expression]
    ) -> list[str]:
        stmt = ["SELECT"]
        if isinstance(exp, list):
            try:
                stmt.append(", ".join(list(col.find(expr.Column).name for col in exp)))
            except AttributeError:
                stmt.append(", ".join(list(col.sql() for col in exp)))
        if isinstance(exp, dict):
            stmt.append(" ".join(col.sql() + "," for col in exp.values()))
        return stmt

    @classmethod
    def group_by(cls, exp: dict[str, expr.Expression]) -> list[str]:
        return ["GROUP BY", ", ".join(col.sql() for col in exp.values())]

    @classmethod
    def from_(cls, exp: expr.Expression | str) -> list[str]:
        stmt = ["FROM"]
        if isinstance(exp, str):
            stmt.append(exp)
        else:
            stmt.append(exp.sql())
        return stmt

    @classmethod
    def where(cls, exp: expr.Expression) -> list[str]:
        return ["WHERE", exp.sql()]

    @classmethod
    def limit(cls, exp: int) -> list[str]:
        return ["LIMIT", str(exp)]

    @classmethod
    def aggregations(cls, exp: t.Sequence[expr.Expression]) -> list[str]:
        return [", ".join(col.sql() for col in exp)]

    @classmethod
    def order_by(cls, exp: list[expr.Expression]) -> list[str]:
        return ["ORDER BY", ", ".join(col.find(expr.Column).name for col in exp)]

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
    def from_query(cls, query: Query):
        plan = planner.Plan(query.ast)

        stages = list()
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
