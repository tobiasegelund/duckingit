import os
import re

import boto3
import duckdb
from sqlglot import parse_one, exp


class Planner:
    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self.conn = conn

    def scan_bucket(self, bucket: str) -> list[str]:
        if bucket[-1] == "/":
            bucket = bucket[:-1]

        # TODO: Count the number of files / size in each prefix to divide the workload better
        data = self.conn.sql(
            f"""
            SELECT DISTINCT
                regexp_replace(file, '/[^/]+$', '') AS prefix
            FROM glob('{bucket}/*')
            """
        ).fetchall()

        return data

    def find_bucket(self, query: str) -> str:
        buckets = parse_one(query).find_all(exp.Table)
        return next(buckets).sql()

    def update_query(self, query: str, list_of_prefixes: list[str]) -> str:
        sub = f"read_parquet({list_of_prefixes})"

        # TODO: Add read_parquet here as well
        # TODO: Lower case everything
        query_upd = re.sub(r"scan_parquet\([^)]*\)", sub, query)

        return query_upd

    def plan(self, query: str, invokations: int) -> list[str]:
        bucket = self.find_bucket(query)

        list_of_prefixes = self.scan_bucket(bucket=bucket)

        # TODO: Heuristic to divide the workload between the invokations

        # TODO: Create loop
        query_upd = self.update_query(query=query, list_of_prefixes=list_of_prefixes)

        # TODO: Remove list here
        return [query_upd]


# class Optimizer:
#     pass

#     def analyze_query(self, query: str):
#         pass
