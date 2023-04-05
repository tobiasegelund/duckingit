import re

import duckdb
from sqlglot import parse_one, exp


def scan_bucket(conn: duckdb.DuckDBPyConnection, bucket: str) -> list[str]:
    if bucket[-1] == "/":
        bucket = bucket[:-1]

    # TODO: Count the number of files in each prefix to divide the workload better
    data = conn.sql(
        f"""
        SELECT DISTINCT
            regexp_replace(file, '/[^/]+$', '') AS prefix
        FROM glob('{bucket}/*')
        """
    ).fetchall()

    return data


def find_bucket(query: str) -> str:
    buckets = parse_one(query).find_all(exp.Table)
    return next(buckets).sql()


def update_query(query: str, list_of_prefixes: list[str]) -> str:
    sub = f"read_parquet({list_of_prefixes})"

    query_upd = re.sub(r"scan_parquet\([^)]*\)", sub, query)

    return query_upd


def analyze_query(query: str, conn: duckdb.DuckDBPyConnection):
    pass


def optimize(
    query: str, conn: duckdb.DuckDBPyConnection, invokations: int
) -> list[str]:
    bucket = find_bucket(query)

    list_of_prefixes = scan_bucket(conn=conn, bucket=bucket)

    # TODO: Heuristic to divide the workload between the invokations

    # TODO: Create loop
    query_upd = update_query(query=query, list_of_prefixes=list_of_prefixes)

    return query_upd
