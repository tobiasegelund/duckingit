import duckdb

from duckingit._parser import Query
from duckingit._planner import Plan


class Dataset:
    """
    The mapping between data objects in buckets and physical. If cache, then in memory
    locally.

    Unmanaged, managed datasets? Able to apply drop?

    Data source class to handle the connection between bucket and session?

    Generator => Only run when told
    """

    def __init__(
        self, conn: duckdb.DuckDBPyConnection, query: Query, execution_plan: Plan
    ) -> None:
        self.conn = conn

    # def __repr__(self) -> str:
    #     return ""

    @classmethod
    def create(
        cls, conn: duckdb.DuckDBPyConnection, query: Query, execution_plan: Plan
    ):
        return cls(conn, query, execution_plan)

    def write(self):
        pass

    def drop(self) -> None:
        pass

    def show(self) -> duckdb.DuckDBPyRelation:
        pass

    def print_shema(self):
        pass


# class WriteSet:
#     pass


# class ReadSet:
#     pass
