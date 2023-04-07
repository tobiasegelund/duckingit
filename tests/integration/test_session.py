import pytest


BUCKET_NAME = "s3-duckdb-tobiasegelund"


@pytest.mark.parametrize(
    "query, invokations, expected",
    [
        (
            f"SELECT * FROM read_parquet(['s3://{BUCKET_NAME}/2023/01/*'])",
            1,
            100,
        ),
        (
            f"SELECT * FROM read_parquet(['s3://{BUCKET_NAME}/2023/*'])",
            1,
            1600,
        ),
    ],
)
def test_DuckSession_execute(query, invokations, expected, session):
    got = session.execute(query=query, invokations=invokations)

    assert len(got.fetchall()) == expected
