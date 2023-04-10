import duckdb
import pytest

# from duckingit._exceptions import WrongInvokationType
from duckingit._parser import QueryParser


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
def test_find_key(query, expected):
    got = QueryParser.find_key(query=query)

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
def test_find_bucket(query, expected):
    got = QueryParser.find_bucket(query=query)

    assert got == expected
