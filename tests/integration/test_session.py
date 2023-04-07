import pytest


@pytest.mark.parametrize(
    "query, invokations, expected",
    [
        (
            "SELECT * FROM read_parquet(['s3://s3-duckdb-tobiasegelund/2023/01/*'])",
            1,
            100,
        ),
        (
            "SELECT * FROM read_parquet(['s3://s3-duckdb-tobiasegelund/2023/*'])",
            1,
            1600,
        ),
    ],
)
def test_DuckSession_execute(query, invokations, expected, session):
    got = session.execute(query=query, invokations=invokations)

    assert len(got.fetchall()) == expected
