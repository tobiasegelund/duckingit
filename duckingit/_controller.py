import typing as t

import duckdb

from duckingit._planner import Plan

# from duckingit._dataset import Dataset
# from duckingit.integrations import AWS

if t.TYPE_CHECKING:
    from duckingit._session import DuckSession


class Controller:
    def execute(
        self, execution_plan: Plan, prefix: str
    ) -> tuple[duckdb.DuckDBPyRelation, str]:
        raise NotImplementedError()


class LocalController(Controller):
    """The purpose of the controller is to control the invokations of
    serverless functions, e.g. Lambda functions.

    It invokes and collects the data, as well as concatenate it altogether before it's
    delivered to the user.

    TODO:
        - Incorporate cache functionality to minimize compute power.
        - Copy from cache?
        - Only select a subset of partitions (minimize throughput)
            Can be based on number of rows or byte size
    """

    def __init__(self, session: "DuckSession") -> None:
        self._session = session

    def fetch_cache_metadata(self):
        pass

    def fetch_cache(self):
        pass

    def check_status_of_invokations(self):
        pass

    def evaluate_execution_plan(self):
        pass

    def execute_plan(self, execution_plan: Plan, prefix: str):
        pass


# class RemoteController(Controller):
#     """Class to communicate with controller running as a serverless function"""

#     pass
