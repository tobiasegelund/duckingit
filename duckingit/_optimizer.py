import duckdb
from sqlglot import parse_one, exp


def scan_bucket():
    pass


def analyze_query():
    pass


def find_tables(sql: str) -> list[str]:
    tables = parse_one(sql).find_all(exp.Table)
    return tables
