"""
https://arrow.apache.org/docs/python/ipc.html
"""
import uuid

import duckdb
import pandas as pd

from ._provider import Provider


class Controller:
    def execute(self, queries: list[str]) -> duckdb.DuckDBPyRelation:
        raise NotImplementedError()


class LocalController(Controller):
    """The purpose of the controller is to control the invokations of
    serverless functions, e.g. Lambda functions.

    It invokes and collects the data, as well as concatenate it altogether before it's
    delivered to the user.

    TODO:
        - Incorporate cache functionality to minimize compute power.
    """

    _cache_prefix = ".cache/duckingit/"

    def __init__(self, conn: duckdb.DuckDBPyConnection, provider: Provider) -> None:
        self.conn = conn
        self.provider = provider

    def _extract_content(self, payload: dict) -> pd.DataFrame:
        # TODO: Find a way to enforce dtypes to the dataframe => Conversion dict?
        data = payload.get("data")
        cols = payload.get("columns")
        dtypes = payload.get("dtypes")

        return pd.DataFrame(data, columns=cols)

    def execute(self, queries: list[str]) -> tuple[duckdb.DuckDBPyRelation, str]:
        payloads = self.provider.invoke(queries=queries)

        dfs = list()
        for payload in payloads:
            _df = self._extract_content(payload=payload)
            dfs.append(_df)

        df = pd.concat(dfs, axis=0).reset_index(drop=True)

        table_name = f"__duckingit_{uuid.uuid1().hex[:6]}"
        self.conn.sql("""CREATE TEMP TABLE {} AS SELECT * FROM df""".format(table_name))

        return self.conn.sql("SELECT * FROM {}".format(table_name)), table_name


# class RemoteController(Controller):
#     """Class to communicate with controller running as a serverless function"""

#     pass
