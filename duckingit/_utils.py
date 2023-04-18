import itertools
import hashlib
import typing as t
import uuid
from collections.abc import Iterable

import duckdb

T = t.TypeVar("T")


def flatten_list(_list: list[list[T]]) -> list[T]:
    """Flats a list of lists

    Args:
        _list, list[list[T]]: A list of list of value T

    Returns:
        A list of value T

    Example:
        >>> flatten_list([[1, 2], [3, 4]])
        [1, 2, 3, 4]

    """
    return list(itertools.chain(*_list))


def split_list_in_chunks(_list: list[str], number_of_invokations: int) -> list[list]:
    """Divides the list evenly among the number of invokations

    Args:
        _list, list[str]: A list of strings

    Returns:
        A list of lists based on the number of invokations, but no more than the length
        of the supplied list

    Examples:
        >>> split_list_in_chunks(["SELECT * FROM table1", "SELECT * FROM table2"], 3)
        [["SELECT * FROM table1"], ["SELECT * FROM table2"]]

        >>> split_list_in_chunks(["SELECT * FROM table1", "SELECT * FROM table2"], 1)
        [["SELECT * FROM table1", "SELECT * FROM table2"]]
    """

    # Must not invoke more functions than number of search queries
    if (size := len(_list)) < number_of_invokations:
        number_of_invokations = size

    k, m = divmod(len(_list), number_of_invokations)
    return [
        _list[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)]
        for i in range(number_of_invokations)
    ]


def create_md5_hash_string(string: str) -> str:
    return hashlib.md5(string.encode(), usedforsecurity=False).hexdigest()


def create_unique_name(prefix: str = "__duckingit") -> str:
    return f"{prefix}_{uuid.uuid1().hex[:6]}"


def ensure_iterable(value: T | t.Iterable[T]) -> list[T]:
    """Ensure to return an iterable

    Args:
        value, T | Iterable[T]: An arbitrary value

    Returns:
        Returns a list of type T, if T is not an iterable

    Examples:
        >>> ensure_iterable([1, 2])
        [1, 2]
        >>> ensure_iterable(1)
        [1]
        >>> ensure_iterable(None)
        []

    """
    if value is None:
        return []

    if not isinstance(value, Iterable):
        return [value]

    return value


def create_duckdb_conn_with_loaded_httpfs() -> duckdb.DuckDBPyConnection:
    """Returns a in memory DuckDB connection with httpfs loaded"""
    conn = duckdb.connect(":memory:")
    conn.execute("LOAD httpfs;")
    return conn


def scan_bucket_for_prefixes(bucket: str) -> list[str]:
    """Scans the a bucket for prefixes using DuckDB

    Args:
        bucket, str: The bucket to scan, e.g. s3://BUCKET_NAME/
    """
    conn = create_duckdb_conn_with_loaded_httpfs()

    # TODO: Count the number of files / size in each prefix to divide the workload better
    glob_query = f"""
        SELECT DISTINCT
            CONCAT(REGEXP_REPLACE(file, '/[^/]+$', ''), '/*') AS prefix
        FROM GLOB({bucket})
        """

    try:
        prefixes = conn.sql(glob_query).fetchall()

    except RuntimeError:
        raise ValueError(
            "Please validate that the FROM statement in the query is correct."
        )

    return flatten_list(prefixes)
