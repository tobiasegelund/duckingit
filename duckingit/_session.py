import duckdb
from enum import Enum

from ._provider import AWS
from ._optimizer import optimize


class Format(Enum):
    PARQUET = "parquet"
    DATAFRAME = "pd.DataFrame"
    ORC = "ORC"
    AVRO = "AVRO"


class DuckSession:
    """Class to handle the session of DuckDB lambda functions"""

    def __init__(
        self,
        function: str = "DuckExecutor",
        # controller_function: str = "DuckController",
        duckdb_config: str = ":memory:",
        invokations_default: int = 1,
        # format: str = "parquet",
        **kwargs
    ) -> None:
        self.provider = AWS(function_name=function)
        self.conn = duckdb.connect(duckdb_config)
        self.invokations_default = invokations_default
        # self.format = format
        self.kwargs = kwargs

        self._install_httpfs()

    def _install_httpfs(self) -> None:
        self.conn.execute("INSTALL httpfs;")

    def execute(self, query: str, *, invokations: int | None = None):
        """Divide the

        Args:
            function_name, Optional(str):
                Defaults to create a new Lambda function
            invokations, int:
                Defaults to 1
        """
        number_of_invokations = (
            invokations if invokations is not None else self.invokations_default
        )

        list_of_queries = optimize(
            query=query, conn=self.conn, invokations=number_of_invokations
        )
