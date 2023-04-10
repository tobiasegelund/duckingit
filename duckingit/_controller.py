"""
https://arrow.apache.org/docs/python/ipc.html
"""
import uuid

import duckdb

from ._provider import Provider


class Controller:
    def execute(
        self, queries: list[str], bucket_name: str, query_hash: str
    ) -> tuple[duckdb.DuckDBPyRelation, str]:
        raise NotImplementedError()


class LocalController(Controller):
    """The purpose of the controller is to control the invokations of
    serverless functions, e.g. Lambda functions.

    It invokes and collects the data, as well as concatenate it altogether before it's
    delivered to the user.

    TODO:
        - Incorporate cache functionality to minimize compute power.
    """

    _CACHE_PREFIX = ".cache/duckingit/"

    def __init__(self, conn: duckdb.DuckDBPyConnection, provider: Provider) -> None:
        self.conn = conn
        self.provider = provider

    def _scan_cached_data(self, query_hash: str):
        pass

    def _create_prefix(self, bucket_name: str, query_hash: str) -> str:
        return f"{bucket_name}/{self._CACHE_PREFIX}/{query_hash}/"

    def execute(
        self, queries: list[str], bucket_name: str, query_hash: str
    ) -> tuple[duckdb.DuckDBPyRelation, str]:
        prefix = self._create_prefix(bucket_name=bucket_name, query_hash=query_hash)
        self.provider.invoke(queries=queries, prefix=prefix)

        table_name = f"__duckingit_{uuid.uuid1().hex[:6]}"
        self.conn.sql(
            """
            CREATE TEMP TABLE {} AS
            SELECT * FROM scan_parquet([{}/*])
            """.format(
                table_name, prefix
            )
        )

        return self.conn.sql("SELECT * FROM {}".format(table_name)), table_name


# class RemoteController(Controller):
#     """Class to communicate with controller running as a serverless function"""

#     pass
