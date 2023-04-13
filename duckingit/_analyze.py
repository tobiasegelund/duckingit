import itertools
import typing as t

import duckdb


T = t.TypeVar("T")


def scan_bucket(bucket: str, conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Scans the URL for files, e.g. a S3 bucket

    Usage:
        Planner(conn=conn).scan_bucket(key="s3://<BUCKET_NAME>/*")
    """
    if bucket[-1] != "*":
        return [bucket]

    # TODO: Count the number of files / size in each prefix to divide the workload better
    glob_query = f"""
        SELECT DISTINCT
            CONCAT(REGEXP_REPLACE(file, '/[^/]+$', ''), '/*') AS prefix
        FROM GLOB('{bucket}')
        """

    try:
        prefixes = conn.sql(glob_query).fetchall()

    except RuntimeError:
        raise ValueError(
            "Please validate that the FROM statement in the query is correct."
        )

    return flatten_list(prefixes)


def flatten_list(_list: list[list[T]]) -> list[T]:
    return list(itertools.chain(*_list))


def split_list_in_chunks(_list: list[str], number_of_invokations: int) -> list[list]:
    # Must not invoke more functions than number of search queries
    if (size := len(_list)) < number_of_invokations:
        number_of_invokations = size

    k, m = divmod(len(_list), number_of_invokations)
    return [
        _list[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)]
        for i in range(number_of_invokations)
    ]
