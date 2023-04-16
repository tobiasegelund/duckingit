import typing as t

import duckdb

from duckingit._parser import Query
from duckingit._planner import Plan

if t.TYPE_CHECKING:
    from duckingit._session import DuckSession


class Dataset:
    """
    The mapping between data objects in buckets and physical. If cache, then in memory
    locally.

    Unmanaged, managed datasets? Able to apply drop?

    Data source class to handle the connection between bucket and session?

    Generator => Only run when told

    TODO: Update metadata on session
    """

    _CACHE_PREFIX = ".cache/duckingit"

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        query: Query,
        execution_plan: Plan,
        session: "DuckSession",
    ) -> None:
        self._conn = conn
        self._query = query
        self._session = session
        self.execution_plan = execution_plan

    # def __repr__(self) -> str:
    #     return ""

    def _create_prefix(self) -> str:
        return f"{self._query.bucket}/{self._CACHE_PREFIX}/{self._query.hashed}"

    def write(self, mode: str):
        pass

    def drop(self) -> None:
        pass

    def show(self) -> duckdb.DuckDBPyRelation:
        duckdb_obj, table_name = self._session._controller.execute(
            execution_plan=self.execution_plan, prefix=self._create_prefix()
        )

        if table_name != "":
            self._session._metadata[table_name] = ""  # query_parsed.sql

        return duckdb_obj

    def print_shema(self):
        pass


# class WriteDataset:
# write_to, str | None: The prefix to write to, e.g. 's3://BUCKET_NAME/data'
#     Defaults to .cache/duckingit/ prefix
#     def mode(self):
#         pass


# class ReadDataset:
#     pass
