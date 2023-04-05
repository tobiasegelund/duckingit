import duckdb
from enum import Enum

from ._provider import AWS
from ._optimizer import optimize


class Format(Enum):
    PARQUET = "parquet"
    DATAFRAME = "pd.DataFrame"


class DuckingIt:
    def __init__(
        self, lambda_function: str, duckdb_config: str = ":memory:", **kwargs
    ) -> None:
        self.provider = AWS(function_name=lambda_function)
        self.conn = duckdb.connect(duckdb_config)
        self.kwargs = kwargs

        self._install_httpfs()

    def _install_httpfs(self) -> None:
        self.conn.execute("INSTALL httpfs;")

    def collect(self, query: str, invokations: int = 1, format: str = "parquet"):
        """Divide the

        Args:
            function_name, Optional(str):
                Defaults to create a new Lambda function
            invokations, int:
                Defaults to 1
        """
        list_of_queries = optimize(query=query, conn=self.conn, invokations=invokations)
