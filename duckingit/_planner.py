import re
import itertools

import duckdb

from ._exceptions import WrongInvokationType
from ._parser import QueryParser


class Planner:
    """Class to plan the workload across nodes

    Basically, the class scan the bucket based on the query, divides the workload on the
    number of invokations and hands the information to the Controller.

    It's the Planner's job to make sure the workload is equally distributed between the
    nodes, as well as validating the query.

    Attributes:
        conn, duckdb.DuckDBPyConnection: Local DuckDB connection

    Methods:
        generate_plan: Creates a query plan that divides the workload between nodes that can be
            used by the Controller
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self.conn = conn

    def _scan_bucket(self, prefix: str) -> list[str]:
        """Scans the URL for files, e.g. a S3 bucket

        Usage:
            Planner(conn=conn).scan_bucket(key="s3://<BUCKET_NAME>/*")
        """
        if prefix[-1] != "*":
            return [prefix]

        # TODO: Count the number of files / size in each prefix to divide the workload better
        glob_query = f"""
            SELECT DISTINCT
                CONCAT(REGEXP_REPLACE(file, '/[^/]+$', ''), '/*') AS prefix
            FROM GLOB('{prefix}')
            """

        try:
            prefixes = self.conn.sql(glob_query).fetchall()

        except RuntimeError:
            raise ValueError(
                "Please validate that the FROM statement in the query is correct."
            )

        return self._flatten_list(_list=prefixes)

    def _flatten_list(self, _list: list) -> list:
        return list(itertools.chain(*_list))

    def _split_list_in_chunks(
        self, _list: list[str], number_of_invokations: int
    ) -> list[list]:
        # Must not invoke more functions than number of search queries
        if (size := len(_list)) < number_of_invokations:
            number_of_invokations = size

        k, m = divmod(len(_list), number_of_invokations)
        return [
            _list[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)]
            for i in range(number_of_invokations)
        ]

    def _update_query(self, query: str, list_of_prefixes: list[str]) -> str:
        sub = f"read_parquet({list_of_prefixes})"

        query_upd = re.sub(r"(?:scan|read)_parquet\([^)]*\)", sub, query)

        return query_upd

    def generate_plan(self, query: str, invokations: int | str) -> list[str]:
        query = QueryParser.parse(query)

        key = QueryParser.find_key(query)
        list_of_prefixes = self._scan_bucket(prefix=key)

        if isinstance(invokations, str):
            if invokations != "auto":
                raise WrongInvokationType(
                    f"The number of invokations can only be 'auto' or an integer. \
{invokations} was provided."
                )
            invokations = len(list_of_prefixes)

        # TODO: Heuristic to divide the workload between the invokations based on size of prefixes / number of files etc.
        # Or based on some deeper analysis of the query?
        list_of_chunks_of_prefixes = self._split_list_in_chunks(
            list_of_prefixes, number_of_invokations=invokations
        )

        updated_queries = list()
        for chunk in list_of_chunks_of_prefixes:
            query_upd = self._update_query(query=query, list_of_prefixes=chunk)
            updated_queries.append(query_upd)
        return updated_queries


# class Optimizer:
#     pass

#     def analyze_query(self, query: str):
#         pass


# from enum import Enum
# class Format(Enum):
#     PARQUET = "parquet"
#     JSON = "json"
#     CSV = "csv"
#     ORC = "orc"
#     AVRO = "avro"
