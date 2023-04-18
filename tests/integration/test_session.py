import pytest


BUCKET_NAME = "s3-duckdb-tobiasegelund"


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            f"SELECT * FROM read_parquet(['s3://{BUCKET_NAME}/2023/01/*'])",
            100,
        ),
        (
            f"SELECT * FROM read_parquet(['s3://{BUCKET_NAME}/2023/*'])",
            1600,
        ),
    ],
)
def test_DuckSession_execute(query, expected, session):
    got = session.execute(query=query)

    assert len(got.fetchall()) == expected
