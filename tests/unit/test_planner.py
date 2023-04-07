import duckdb
import pytest

from duckingit._exceptions import WrongInvokationType


@pytest.fixture
def planner(MockPlanner):
    conn = duckdb.connect(":memory:")
    planner = MockPlanner(conn=conn)
    yield planner


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
def test_find_key(query, expected, planner):
    got = planner.find_key(query=query)

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
        (
            "SELECT * FROM read_parquet(['s3://<BUCKET_NAME>/2023/02/test2.parquet']) WHERE 1=1",
            ["s3://<BUCKET_NAME>/2023/02/test2.parquet"],
            "SELECT * FROM read_parquet(['s3://<BUCKET_NAME>/2023/02/test2.parquet']) WHERE 1=1",
        ),
    ],
)
def test_update_query(query, prefix, expected, planner):
    got = planner.update_query(query=query, list_of_prefixes=prefix)

    assert got == expected


@pytest.mark.parametrize(
    "query, expected, invokations",
    [
        (
            "SELECT * FROM scan_parquet(['s3://<BUCKET_NAME>/2023/*'])",
            [
                "SELECT * FROM read_parquet(['s3://<BUCKET_NAME>/2023/01/*', 's3://<BUCKET_NAME>/2023/02/*', 's3://<BUCKET_NAME>/2023/03/*'])"
            ],
            1,
        ),
        (
            "SELECT * FROM scan_parquet(['s3://<BUCKET_NAME>/2023/*']) WHERE 1=1",
            [
                "SELECT * FROM read_parquet(['s3://<BUCKET_NAME>/2023/01/*', 's3://<BUCKET_NAME>/2023/02/*', 's3://<BUCKET_NAME>/2023/03/*']) WHERE 1=1"
            ],
            1,
        ),
        (
            "SELECT * FROM scan_parquet(['s3://<BUCKET_NAME>/2023/*']) WHERE 1=1",
            [
                "SELECT * FROM read_parquet(['s3://<BUCKET_NAME>/2023/01/*', 's3://<BUCKET_NAME>/2023/02/*']) WHERE 1=1",
                "SELECT * FROM read_parquet(['s3://<BUCKET_NAME>/2023/03/*']) WHERE 1=1",
            ],
            2,
        ),
        (
            "SELECT * FROM scan_parquet(['s3://<BUCKET_NAME>/2023/*']) WHERE 1=1",
            [
                "SELECT * FROM read_parquet(['s3://<BUCKET_NAME>/2023/01/*']) WHERE 1=1",
                "SELECT * FROM read_parquet(['s3://<BUCKET_NAME>/2023/02/*']) WHERE 1=1",
                "SELECT * FROM read_parquet(['s3://<BUCKET_NAME>/2023/03/*']) WHERE 1=1",
            ],
            3,
        ),
        (
            "SELECT * FROM scan_parquet(['s3://<BUCKET_NAME>/2023/*']) WHERE 1=1",
            [
                "SELECT * FROM read_parquet(['s3://<BUCKET_NAME>/2023/01/*']) WHERE 1=1",
                "SELECT * FROM read_parquet(['s3://<BUCKET_NAME>/2023/02/*']) WHERE 1=1",
                "SELECT * FROM read_parquet(['s3://<BUCKET_NAME>/2023/03/*']) WHERE 1=1",
            ],
            "auto",
        ),
    ],
)
def test_plan(query, expected, invokations, planner):
    got = planner.plan(query=query, invokations=invokations)

    assert got == expected


def test_plan_error(planner):
    got = False

    query = "SELECT * FROM scan_parquet(['s3://<BUCKET_NAME>/2023/*']) WHERE 1=1"
    try:
        _ = planner.plan(query=query, invokations="1")
    except WrongInvokationType:
        got = True

    assert got
