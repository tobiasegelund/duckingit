import os

import duckdb

from ._controller import Controller, LocalController
from ._planner import Plan, Step
from ._parser import Query
from ._provider import AWS
from ._analyze import scan_bucket
from ._config import DuckConfig


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
        enable_cache: bool = True,
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
            enable_cache, bool: Caches client-side, ie. caches are stored in memory on
                the machine running the DuckSession. Defaults to True
            **kwargs
        """
        self._function_name = function_name
        self._invokations_default = invokations_default
        self._enable_cache = enable_cache
        self._kwargs = kwargs

        self._conn = duckdb.connect(**duckdb_config)
        self._load_httpfs()
        self._set_credentials()

        self._controller = self._set_controller()

        self._conf: DuckConfig | None = None
        self._metadata: dict[str, str] = dict()

    @property
    def metadata(self) -> dict[str, str]:
        return self._metadata

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        return self._conn

    @property
    def conf(self) -> DuckConfig:
        if self._conf is None:
            self._conf = DuckConfig(function_name=self._function_name)
        return self._conf

    def _set_controller(self) -> Controller:
        return LocalController(
            conn=self._conn,
            provider=AWS(function_name=self._function_name),
            enable_cache=self._enable_cache,
        )

    def _load_httpfs(self) -> None:
        self._conn.execute("INSTALL httpfs; LOAD httpfs;")

    def _set_credentials(self) -> None:
        # https://duckdb.org/docs/sql/configuration.html
        # TODO: Must be more generic to work with other providers
        self._conn.execute(
            f"""
            SET s3_region='{os.getenv("AWS_DEFAULT_REGION", None)}';
            SET s3_access_key_id='{os.getenv("AWS_ACCESS_KEY_ID", None)}';
            SET s3_secret_access_key='{os.getenv("AWS_SECRET_ACCESS_KEY", None)}';
            """
        )

    def _create_prefix(self, query: Query, write_to: str | None) -> str:
        if write_to is not None:
            if write_to[-1] != "/":
                return write_to
            return write_to[:-1]
        return f"{query.bucket}/{self._CACHE_PREFIX}/{query.hashed}"

    def execute(
        self, query: str, *, invokations: int | None = None, write_to: str | None = None
    ) -> duckdb.DuckDBPyRelation:
        """Execute the query against a number of serverless functions

        Args:
            query, str: DuckDB SQL query to run
            invokations, int | None: The number of invokations of the Lambda function
                Defaults to 'auto' (See initialization of the session class)
            write_to, str | None: The prefix to write to, e.g. 's3://BUCKET_NAME/data'
                Defaults to .cache/duckingit/ prefix
        """
        query_parsed: Query = Query.parse(query)
        query_parsed.list_of_prefixes = scan_bucket(query_parsed.source, conn=self.conn)

        number_of_invokations = (
            invokations if invokations is not None else self._invokations_default
        )
        execution_plan = Plan.create_from_query(
            query=query_parsed, invokations=number_of_invokations
        )

        prefix = self._create_prefix(query=query_parsed, write_to=write_to)
        duckdb_obj, table_name = self._controller.execute(
            execution_plan=execution_plan, prefix=prefix
        )

        # Update metadata
        if table_name != "":
            self._metadata[table_name] = query_parsed.sql

        return duckdb_obj
