import hashlib
import itertools
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
        _list[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(number_of_invokations)
    ]


def create_hash_string(
    string: str, algorithm: str = "md5", digits: int | None = None, first_char: str = ""
) -> str:
    algo = getattr(hashlib, algorithm)
    val = algo(string.encode(), usedforsecurity=False).hexdigest()
    if digits is None:
        return first_char + val
    return first_char + val[:digits]


def create_unique_name(prefix: str = "__duckingit") -> str:
    return f"{prefix}_{uuid.uuid1().hex[:6]}"


def ensure_iterable(value: T | t.Iterable[T]) -> Iterable[T]:
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


def create_conn_with_httpfs_loaded() -> duckdb.DuckDBPyConnection:
    """Returns a in memory DuckDB connection with httpfs loaded"""
    from duckingit.providers import Providers

    conn = duckdb.connect(":memory:")
    conn.execute("LOAD httpfs;")
    conn.execute(Providers.get_or_raise("aws").duckdb_settings())

    return conn


def scan_source_for_files(source: str) -> list[str]:
    """Scans the source for files using DuckDB

    Args:
        source, str: The source to scan, e.g. s3://BUCKET_NAME/

    """
    conn = create_conn_with_httpfs_loaded()

    if source[-1] == "/":
        source = source[:-1]

    files = conn.sql(
        f"SELECT REGEXP_EXTRACT(file, '[0-9a-f]{{32}}') AS file FROM GLOB('{source}/*')"
    ).fetchall()
    return flatten_list(files)


def scan_source_for_prefixes(source: str) -> list[str]:
    """Scans the a source for prefixes using DuckDB

    Args:
        source, str: The source to scan, e.g. s3://source_NAME/
    """
    conn = create_conn_with_httpfs_loaded()

    # TODO: Count the number of files / size in each prefix to divide the workload better
    glob_query = f"""
        SELECT DISTINCT
            CONCAT(REGEXP_REPLACE(file, '/[^/]+$', ''), '/*') AS prefix
        FROM GLOB({source})
        """

    prefixes = conn.sql(glob_query).fetchall()

    return flatten_list(prefixes)


def scan_source_parquet_metadata(source: str) -> list[tuple[str, int]]:
    """Scans metadata of parquet files at source using DuckDB

    Args:
        source, str: The source to scan, e.g. s3://BUCKET_NAME/

    """
    conn = create_conn_with_httpfs_loaded()

    if source[-1] == "/":
        source = source[:-1]

    query = f"""
        SELECT file_name, SUM(total_compressed_size) AS bytes
        FROM  PARQUET_METADATA('{source}/*')
        GROUP BY file_name
    """

    files = conn.sql(query).fetchall()
    return files


def cast_mapping_to_string_with_newlines(service_name: str, mapping: dict[str, t.Any]):
    map_key_with_value = list(
        ".".join([service_name, k]) + ":" + str(v) for k, v in mapping.items()
    )
    repr = "\n".join(map_key_with_value)
    return repr
