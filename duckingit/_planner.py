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
    def create(cls, query: Query, prefixes: list[str]):
        # TODO: Update to use Format Enum
        subquery = query.sql.replace(query._source, f"READ_PARQUET({prefixes})")

        return cls(subquery=subquery, subquery_hashed=create_md5_hash_string(subquery))


class Plan:
    """Class to create an execution plan across nodes

    The execution plan consists of a execution steps based on queries. Basically, the
    class scan the bucket based on the query, divides the workload on the number of
    invokations. Afterwards, its the Controller's job to execute the plan.

    Methods:
        create_from_query: Creates an execution plan that divides the workload between
            nodes
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
            execution_steps.append(Step.create(query=query, prefixes=chunk))

        return cls(query=query, execution_steps=execution_steps)

    def __repr__(self) -> str:
        return f"{list(step for step in self.execution_steps)}"


# class Optimizer:
#     pass

#     def analyze_query(self, query: str):
#         pass
