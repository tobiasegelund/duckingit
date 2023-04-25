import typing as t
import copy
from dataclasses import dataclass

from sqlglot import planner

from duckingit._exceptions import WrongInvokationType
from duckingit._parser import Query
from duckingit._utils import split_list_in_chunks, create_hash_string


@dataclass
class Step:
    subquery: str
    subquery_hashed: str

    @classmethod
    def create(cls, query: Query, prefixes: list[str]):
        """Creates a step to execute on a serverless function

        Args:
            query, Query: A query parsed by the Query class
            prefixes, list[str]: A list of prefixes to scan

        Returns:
            Step<SUBQUERY | SUBQUERY_HASHED>

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
                    subquery = subquery.replace(table, f"READ_{extension}_AUTO({prefixes})")

        return cls(subquery=subquery, subquery_hashed=create_hash_string(subquery))

    def __repr__(self) -> str:
        return f"Step<QUERY='{self.subquery}' | HASH='{self.subquery_hashed}'>"


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

    def __init__(self, query: Query, execution_steps: list[Step]) -> None:
        self.query = query
        self.execution_steps = execution_steps

    def __len__(self) -> int:
        return len(self.execution_steps)

    @classmethod
    def create_from_query(cls, query: Query, invokations: int | str):
        if isinstance(invokations, str):
            if invokations != "auto":
                raise WrongInvokationType("`invokations` can only be 'auto' or an integer")
            invokations = len(query.list_of_prefixes)

        # TODO: Heuristic to divide the workload between the invokations based on size
        # of prefixes / number of files etc. Or based on some deeper analysis of the query?
        chunks_of_prefixes = split_list_in_chunks(
            query.list_of_prefixes, number_of_invokations=invokations
        )

        execution_steps: list[Step] = []
        for chunk in chunks_of_prefixes:
            execution_steps.append(Step.create(query=query, prefixes=chunk))

        return cls(query=query, execution_steps=execution_steps)

    def __repr__(self) -> str:
        return f"{list(step for step in self.execution_steps)}"

    def copy(self):
        """Returns a deep copy of the object itself"""
        return copy.deepcopy(self)


class Stage:
    dependencies: t.Iterable[str]
    dependents: t.Iterable[str]

    @classmethod
    def create(cls, node: planner.Plan):
        pass

    def add_dependency(self) -> None:
        pass

    def add_dependents(self) -> None:
        pass


@dataclass
class Operation:
    dependencies: t.Iterable[str]
    dependents: t.Iterable[str]


class Scan(Operation):
    @classmethod
    def create(cls, node: planner.Scan):
        pass


class Sort(Operation):
    pass


class Set(Operation):
    pass


class Aggregate(Operation):
    pass


class Join(Operation):
    pass
