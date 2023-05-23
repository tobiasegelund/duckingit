import datetime

import duckdb

from duckingit._config import DuckConfig
from duckingit._dataset import Dataset
from duckingit._parser import Query
from duckingit._planner import Plan
from duckingit.providers import Providers


class DuckSession:
    """Entrypoint to a session of serverless DuckDB instances

    The main objective of this class is to handle the session and serve as the primary
    entrypoint to a cluster of serverless functions that utilize DuckDB. Its core
    functionality involves planning, distributing, and scaling the number of DuckDB
    instances, as well as merging the results before returning them to the query issuer.

    Attributes:
        conn, duckdb.DuckDBPyConnection: The initialized DuckDB connection
        metadata, dict: Metadata on temporary tables created using the DuckSession

    Methods: TODO: Switch the methods logic? Perhaps more logical
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
        conf: DuckConfig | None = None,
        **kwargs,
    ) -> None:
        """Session of serverless DuckDB instances

        Args:
            conf, DuckConfig: A collection of configuration settings defined using
                the class DuckConfig
            **kwargs
        """
        if conf is not None:
            conf.update()  # Update configuration settings

        self._kwargs = kwargs

        self._conn = duckdb.connect(**self.conf.duckdb.__dict__)
        self._load_httpfs()
        self._set_credentials()

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
        return DuckConfig()

    def _load_httpfs(self) -> None:
        self._conn.execute("INSTALL httpfs; LOAD httpfs;")

    def _set_credentials(self) -> None:
        self._conn.execute(Providers.get_or_raise(self.conf.session.provider).duckdb_settings())

    def sql(self, query: str) -> Dataset:
        """Creates a Dataset to execute against DuckDB instances

        The Dataset can also be configured to save to a specific path or temporary table
        using the write method of the Dataset class.

        Args:
            query, str: DuckDB SQL query to run

        Returns:
            Dataset or duckdb.DuckDBPyRelation if the data already exists in memeory
        """

        # First try DuckDB to see if it can used from there? Will this be confusing?

        parsed_query = Query.parse(query)
        execution_plan = Plan.from_query(query=parsed_query)

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
