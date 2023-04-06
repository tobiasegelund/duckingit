import re
import itertools

import duckdb
from sqlglot import parse_one, exp


class Planner:
    # TODO: Validate that the prefix/file exists else raise Exception
    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self.conn = conn

    def scan_bucket(self, key: str) -> list[str]:
        if key[-1] != "*":
            return [key]

        # TODO: Count the number of files / size in each prefix to divide the workload better
        # TODO: Consider which of 2023/01/* or 2023/01* is most acceptable - a logic that can take both?
        glob_query = f"""
            SELECT DISTINCT
                CONCAT(REGEXP_REPLACE(file, '/[^/]+$', ''), '/*') AS prefix
            FROM GLOB('{key}')
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

    def find_key(self, query: str) -> str:
        # TODO: Update exceptions to user-defined exceptions
        scan_statements = parse_one(query).find_all(exp.Table)
        scan_statement = next(scan_statements).sql()

        # TODO: Update pattern for other filesystems
        pattern = r"\(.*?(s3://[^/]+/.+(/\*|\.parquet|\*)).*?\)"
        match = re.search(pattern, scan_statement)

        if match is None:
            raise ValueError()

        return match.group(1)

    def update_query(self, query: str, list_of_prefixes: list[str]) -> str:
        sub = f"read_parquet({list_of_prefixes})"

        # TODO: Add read_parquet here as well
        # TODO: Make lowercase
        query_upd = re.sub(r"scan_parquet\([^)]*\)", sub, query)

        return query_upd

    def plan(self, query: str, invokations: int) -> list[str]:
        key = self.find_key(query)

        list_of_prefixes = self.scan_bucket(key=key)

        # TODO: Heuristic to divide the workload between the invokations

        # TODO: Create loop
        query_upd = self.update_query(query=query, list_of_prefixes=list_of_prefixes)

        # TODO: Remove list here
        return [query_upd]


class MockPlanner(Planner):
    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self.conn = conn

    def scan_bucket(self, key: str) -> list[str]:
        return [
            "s3://<BUCKET_NAME>/2023/01/*",
            "s3://<BUCKET_NAME>/2023/02/*",
            "s3://<BUCKET_NAME>/2023/03/*",
        ]


# class Optimizer:
#     pass

#     def analyze_query(self, query: str):
#         pass
