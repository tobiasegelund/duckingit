import re
from dataclasses import dataclass

from ._exceptions import WrongInvokationType
from ._parser import Query
from ._encode import create_md5_hash_string
from ._analyze import split_list_in_chunks


@dataclass
class Step:
    subquery: str
    subquery_hashed: str

    @classmethod
    def create(cls, query: str, prefixes: list[str]):
        sub = f"READ_PARQUET({prefixes})"

        subquery = re.sub(r"(?:SCAN|READ)_PARQUET\(LIST_VALUE\(\([^)]*\)", sub, query)

        return cls(subquery=subquery, subquery_hashed=create_md5_hash_string(subquery))


class Plan:
    """Class to plan the workload across nodes

    Basically, the class scan the bucket based on the query, divides the workload on the
    number of invokations and hands the information to the Controller.

    It's the Planner's job to make sure the workload is equally distributed between the
    nodes, as well as validating the query.

    Methods:
        create: Creates a query plan that divides the workload between nodes that can be
            used by the Controller
    """

    def __init__(self, query: Query, execution_steps: list[Step]) -> None:
        self.query = query
        self.execution_steps = execution_steps

    @classmethod
    def create_from_query(cls, query: Query, invokations: int | str):
        if isinstance(invokations, str):
            if invokations != "auto":
                raise WrongInvokationType(
                    f"The number of invokations can only be 'auto' or an integer. \
{invokations} was provided."
                )
            invokations = len(query.list_of_prefixes)

        # TODO: Heuristic to divide the workload between the invokations based on size of prefixes / number of files etc.
        # Or based on some deeper analysis of the query?
        chunks_of_prefixes = split_list_in_chunks(
            query.list_of_prefixes, number_of_invokations=invokations
        )

        execution_steps: list[Step] = []
        for chunk in chunks_of_prefixes:
            execution_steps.append(Step.create(query=query.sql, prefixes=chunk))

        return cls(query=query, execution_steps=execution_steps)


# class Optimizer:
#     pass

#     def analyze_query(self, query: str):
#         pass
