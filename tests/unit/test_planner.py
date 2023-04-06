import duckdb
import pytest

from duckingit._planner import Planner


@pytest.fixture
def planner():
    conn = duckdb.connect(":memory:")
    yield Planner(conn=conn)


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            "SELECT * FROM scan_parquet(['s3://<BUCKET_NAME>/2023/02/test2.parquet'])",
            "SCAN_PARQUET(ARRAY('s3://<BUCKET_NAME>/2023/02/test2.parquet'))",
        ),
        (
            "SELECT * FROM scan_parquet(['s3://<BUCKET_NAME>/2023/02/test2.parquet']) WHERE 1=1",
            "SCAN_PARQUET(ARRAY('s3://<BUCKET_NAME>/2023/02/test2.parquet'))",
        ),
    ],
)
def test_find_bucket(query, expected, planner):
    got = planner.find_bucket(query=query)

    assert got == expected


@pytest.mark.parametrize(
    "query, prefix, expected",
    [
        (
            "SELECT * FROM scan_parquet(['s3://<BUCKET_NAME>/2023/02/test2.parquet'])",
            ["s3://<BUCKET_NAME>/2023/02/test2.parquet"],
            "SELECT * FROM read_parquet(['s3://<BUCKET_NAME>/2023/02/test2.parquet'])",
        ),
        (
            "SELECT * FROM scan_parquet(['s3://<BUCKET_NAME>/2023/02/test2.parquet']) WHERE 1=1",
            ["s3://<BUCKET_NAME>/2023/02/test2.parquet"],
            "SELECT * FROM read_parquet(['s3://<BUCKET_NAME>/2023/02/test2.parquet']) WHERE 1=1",
        ),
        # (
        #     "SELECT * FROM scan_parquet(['s3://<BUCKET_NAME>/2023/02/test2.parquet'], filename=true) WHERE 1=1",
        #     ["s3://<BUCKET_NAME>/2023/02/test2.parquet"],
        #     "SELECT * FROM read_parquet(['s3://<BUCKET_NAME>/2023/02/test2.parquet'], filename=true) WHERE 1=1",
        # ),
    ],
)
def test_update_query(query, prefix, expected, planner):
    got = planner.update_query(query=query, list_of_prefixes=prefix)

    assert got == expected
