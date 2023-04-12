import duckdb
import pytest

# from duckingit._exceptions import WrongInvokationType
from duckingit._parser import Query


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            "SELECT * FROM scan_parquet(['s3://<BUCKET_NAME>/2023/02/test2.parquet'])",
            "s3://<BUCKET_NAME>/2023/02/test2.parquet",
        ),
        (
            "SELECT * FROM scan_parquet(['s3://<BUCKET_NAME>/2023/02/test2.parquet']) WHERE 1=1",
            "s3://<BUCKET_NAME>/2023/02/test2.parquet",
        ),
        (
            "SELECT * FROM read_parquet(['s3://<BUCKET_NAME>/2023/02/test2.parquet']) WHERE 1=1",
            "s3://<BUCKET_NAME>/2023/02/test2.parquet",
        ),
        (
            "SELECT * FROM scan_parquet(['s3://<BUCKET_NAME>/2023/01/*'], filename=true)",
            "s3://<BUCKET_NAME>/2023/01/*",
        ),
        (
            "SELECT * FROM read_parquet(['s3://<BUCKET_NAME>/2023/01/*'], filename=true)",
            "s3://<BUCKET_NAME>/2023/01/*",
        ),
    ],
)
def test_source(query, expected):
    got = Query.parse(query).source

    assert got == expected


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            "SELECT * FROM scan_parquet(['s3://BUCKET_NAME/2023/02/test2.parquet'])",
            "s3://BUCKET_NAME",
        ),
        (
            "SELECT * FROM scan_parquet(['s3://BUCKET_NAME/2023/02/test2.parquet']) WHERE 1=1",
            "s3://BUCKET_NAME",
        ),
        (
            "SELECT * FROM read_parquet(['s3://BUCKET_NAME/2023/02/test2.parquet']) WHERE 1=1",
            "s3://BUCKET_NAME",
        ),
        (
            "SELECT * FROM scan_parquet(['s3://BUCKET_NAME/2023/01/*'], filename=true)",
            "s3://BUCKET_NAME",
        ),
        (
            "SELECT * FROM read_parquet(['s3://BUCKET_NAME/2023/01/*'], filename=true)",
            "s3://BUCKET_NAME",
        ),
    ],
)
def test_bucket(query, expected):
    got = Query.parse(query).bucket

    assert got == expected


@pytest.mark.parametrize(
    "query, expected",
    [
        ("SELECT * FROM\n TABLE", "SELECT * FROM TABLE"),
        ("SELECT\t\t * FROM TABLE", "SELECT * FROM TABLE"),
        (
            """
        SELECT *
        FROM TABLE
        WHERE 1=1
        """,
            "SELECT * FROM TABLE WHERE 1 = 1",
        ),
    ],
)
def test_remove_newlines_and_tabs(query, expected):
    got = Query.parse(query=query).sql

    assert got == expected
