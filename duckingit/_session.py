import os
import datetime

import duckdb

from duckingit._config import DuckConfig
from duckingit._dataset import Dataset
from duckingit._parser import Query
from duckingit._planner import Plan
from duckingit.integrations import Providers


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
        sql: Returns a Dataset class with the exection plan stored
        execute: Creates and execute a Dataset class using .show method to see the result

    Usage:
        >>> session = DuckSession()
        >>> resp = session.sql(query="SELECT * FROM scan_parquet(['s3::/<BUCKET_NAME>/*'])")
        >>> resp.show()

        >>> session.execute(query="SELECT * FROM scan_parquet(['s3::/<BUCKET_NAME>/*'])")
    """

    def __init__(
        self,
        function_name: str = "DuckExecutor",
        duckdb_config: dict = {"database": ":memory:", "read_only": False},
        **kwargs,
    ) -> None:
        """Session of serverless DuckDB instances

        Args:
            function_name, str: The name of the serverless function
                Defaults to "DuckExecutor"
            duckdb_config, dict: DuckDB configurations of the connection. Please take a
                look on their documention. Defaults to {"database": ":memory:",
                "read_only": False}
            **kwargs
        """
        self._function_name = function_name
        self._kwargs = kwargs

        self._conn = duckdb.connect(**duckdb_config)
        self._load_httpfs()
        self._set_credentials()

        self._set_conf()

        self.metadata: dict[str, str] = dict()
        self.metadata_cached: dict[str, datetime.datetime] = {}

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        return self._conn

    # @property
    # def read(self):
    #     pass

    @property
    def conf(self) -> DuckConfig:
        return self._conf

    def _set_conf(self) -> None:
        self._conf = DuckConfig(function_name=self._function_name)

    def _load_httpfs(self) -> None:
        self._conn.execute("INSTALL httpfs; LOAD httpfs;")

    def _set_credentials(self) -> None:
        self._conn.execute(Providers.AWS.credentials)

    def sql(self, query: str) -> Dataset:
        """Creates a Dataset to execute against DuckDB instances

        The Dataset can also be configured to save to a specific path or temporary table
        using the write method of the Dataset class.

        Args:
            query, str: DuckDB SQL query to run

        Returns:
            Dataset class
        """

        # First try DuckDB to see if it can used from there? Will this be confusing?
        # try:
        #     self.conn.sql(query)
        # except Exception as e:

        number_of_invokations = "auto"
        if getattr(self.conf, "_max_invokations") is not None:
            number_of_invokations = self.conf._max_invokations

        if number_of_invokations is None:
            raise ValueError("Number of invokations cannot be valueNone")

        parsed_query = Query.parse(query)
        execution_plan = Plan.create_from_query(
            query=parsed_query, invokations=number_of_invokations
        )

        return Dataset(
            execution_plan=execution_plan,
            session=self,
        )

    def execute(self, query: str) -> duckdb.DuckDBPyRelation:
        """Execute the query using DuckDB instances

        Args:
            query, str: DuckDB SQL query to run

        Returns:
            A duckdb.DuckDBPyRelation showing the queried data
        """
        dataset = self.sql(query=query)

        return dataset.show()
