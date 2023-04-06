import os

import duckdb

from ._controller import LocalController
from ._planner import Planner
from ._provider import AWS


class DuckSession:
    """Class to handle the session of DuckDB lambda functions"""

    def __init__(
        self,
        function_name: str = "DuckExecutor",
        # controller_function: str = "DuckController",
        duckdb_config: str = ":memory:",
        invokations_default: int = 1,
        # format: str = "parquet",
        **kwargs,
    ) -> None:
        self._invokations_default = invokations_default
        # self.format = format
        self._kwargs = kwargs

        self._conn = duckdb.connect(duckdb_config)
        self._load_httpfs()
        self._set_credentials()

        self._controller = LocalController(
            conn=self._conn, provider=AWS(function_name=function_name)
        )
        self._planner = Planner(conn=self._conn)

        self._metadata: dict[str, str] = dict()

    @property
    def metadata(self) -> dict[str, str]:
        return self._metadata

    def _load_httpfs(self) -> None:
        self._conn.execute("INSTALL httpfs; LOAD httpfs;")

    def _set_credentials(self) -> None:
        # TODO: Must be more generic to work on other providers
        self._conn.execute(
            f"""
            SET s3_region='{os.getenv("AWS_DEFAULT_REGION", None)}';
            SET s3_access_key_id='{os.getenv("AWS_ACCESS_KEY_ID", None)}';
            SET s3_secret_access_key='{os.getenv("AWS_SECRET_ACCESS_KEY", None)}';
            """
        )

    def _create_execution_plan(self, query: str, invokations: int) -> list[str]:
        list_of_queries = self._planner.plan(query=query, invokations=invokations)

        return list_of_queries

    def execute(
        self, query: str, *, invokations: int | None = None
    ) -> duckdb.DuckDBPyRelation:
        """Execute query

        Args:
            function_name, Optional(str):
                Defaults to create a new Lambda function
            invokations, int:
                Defaults to 1
        """
        number_of_invokations = (
            invokations if invokations is not None else self._invokations_default
        )

        execution_plan = self._create_execution_plan(
            query=query, invokations=number_of_invokations
        )

        duckdb_obj, table_name = self._controller.execute(queries=execution_plan)

        # Update metadata
        self._metadata[table_name] = query

        return duckdb_obj
