"""
https://arrow.apache.org/docs/python/ipc.html
"""
import duckdb

from ._provider import Provider


class Controller:
    pass


class LocalController(Controller):
    """The purpose of the controller is to control the invokations of
    serverless functions, e.g. Lambda functions.

    It invokes and collects the data, as well as concatenate it altogether before it's
    delivered to the user.

    TODO:
        - Incorporate cache functionality to minimize compute power.
        - Create Temp views of data?
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection, provider: Provider) -> None:
        self.conn = conn
        self.provider = provider

    def _parse(self) -> None:
        pass

    def invoke(self, queries: list[str]) -> None:
        data = self.invoke(queries=queries)


class RemoteController(Controller):
    """Class to communicate with controller running as a serverless function"""

    pass
