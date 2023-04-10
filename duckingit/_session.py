import os

import duckdb

from ._controller import Controller, LocalController
from ._planner import Planner
from ._parser import QueryParser
from ._provider import AWS


class DuckSession:
    """Entrypoint to a session of serverless DuckDB instances

    The main objective of this class is to handle the session and serve as the primary
    entrypoint to a cluster of serverless functions that utilize DuckDB. Its core
    functionality involves planning, distributing, and scaling the number of DuckDB
    instances, as well as merging the results before returning them to the query issuer.

    Attributes:
        conn, duckdb.DuckDBPyConnection: The initialized DuckDB connection
        metadata, dict: Metadata on temporary tables created using the DuckSession

    Methods:
        execute: Execute a DuckDB SQL query concurrently using X number of invokations


    Usage:
        >>> session = DuckSession()
        >>> resp = session.execute(query="SELECT * FROM scan_parquet(['s3::/<BUCKET_NAME>/*'])")
        >>> resp.show()
    """

    _CACHE_PREFIX = ".cache/duckingit"

    def __init__(
        self,
        function_name: str = "DuckExecutor",
        # controller_function: str = "DuckController",
        duckdb_config: dict = {"database": ":memory:", "read_only": False},
        invokations_default: int | str = "auto",
        # format: str = "parquet",
        **kwargs,
    ) -> None:
        """Initiliaze a session of serverless DuckDB instances

        Args:
            function_name, str: The name of the serverless function
                Defaults to "DuckExecutor"
            duckdb_config, dict: DuckDB configurations of the connection. Please take a
                look on their documention.
                Defaults to {"database": ":memory:", "read_only": False}
            invokations_default, int | 'auto': The default number of invokations.
                Defaults to 'auto'
            **kwargs
        """
        self._function_name = function_name
        self._invokations_default = invokations_default
        # self.format = format
        self._kwargs = kwargs

        self._conn = duckdb.connect(**duckdb_config)
        self._load_httpfs()
        self._set_credentials()

        self._controller = self._set_controller()
        self._planner = self._set_planner()

        self._metadata: dict[str, str] = dict()

    @property
    def metadata(self) -> dict[str, str]:
        return self._metadata

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        return self._conn

    def _set_planner(self) -> Planner:
        return Planner(conn=self._conn)

    def _set_controller(self) -> Controller:
        return LocalController(
            conn=self._conn, provider=AWS(function_name=self._function_name)
        )

    def _load_httpfs(self) -> None:
        self._conn.execute("INSTALL httpfs; LOAD httpfs;")

    def _set_credentials(self) -> None:
        # https://duckdb.org/docs/sql/configuration.html
        # TODO: Must be more generic to work on other providers
        self._conn.execute(
            f"""
            SET s3_region='{os.getenv("AWS_DEFAULT_REGION", None)}';
            SET s3_access_key_id='{os.getenv("AWS_ACCESS_KEY_ID", None)}';
            SET s3_secret_access_key='{os.getenv("AWS_SECRET_ACCESS_KEY", None)}';
            """
        )

    def _create_execution_plan(self, query: str, invokations: int | str) -> list[str]:
        list_of_queries = self._planner.generate_plan(
            query=query, invokations=invokations
        )

        return list_of_queries

    def _create_prefix(
        self, bucket_name: str, query_hash: str, write_to: str | None
    ) -> str:
        if write_to is not None:
            if write_to[-1] != "/":
                return write_to
            return write_to[:-1]
        return f"{bucket_name}/{self._CACHE_PREFIX}/{query_hash}"

    def execute(
        self, query: str, *, invokations: int | None = None, write_to: str | None = None
    ) -> duckdb.DuckDBPyRelation:
        """Execute the query against a number of serverless functions

        Args:
            query, str: DuckDB SQL query to run
                Defaults to create a new Lambda function
            invokations, int | None:
                Defaults to 'auto' (See initialization of the session class)
            write_to, str | None: The prefix to write to. E.g. 's3://BUCKET_NAME/2023/01'
        """
        number_of_invokations = (
            invokations if invokations is not None else self._invokations_default
        )

        execution_plan = self._create_execution_plan(
            query=query, invokations=number_of_invokations
        )
        query_hashed = QueryParser.hash_query(query=query)
        bucket_name = QueryParser.find_bucket(query=query)

        prefix = self._create_prefix(
            bucket_name=bucket_name, query_hash=query_hashed, write_to=write_to
        )

        duckdb_obj, table_name = self._controller.execute(
            queries=execution_plan, prefix=prefix
        )

        # Update metadata
        self._metadata[table_name] = query

        return duckdb_obj
