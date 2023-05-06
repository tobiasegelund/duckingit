import pytest

from duckingit._exceptions import DatasetExistError

BUCKET_NAME = "s3-duckdb-tobiasegelund"


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            "SELECT * FROM READ_PARQUET(['s3://s3-duckdb-tobiasegelund/2023/01/*'])",
            100,
        ),
        (
            "SELECT * FROM READ_PARQUET(['s3://s3-duckdb-tobiasegelund/2023/*'])",
            1600,
        ),
    ],
)
def test_DuckSession_execute(query, expected, session):
    got = session.sql(query=query).show()

    assert len(got.fetchall()) == expected


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            "SELECT * FROM READ_PARQUET(['s3://s3-duckdb-tobiasegelund/2023/01/*'])",
            100,
        ),
    ],
)
def test_DuckSession_save_as_temp_table(query, expected, session):
    session.sql(query=query).write.save_as_temp_table("test")

    got = session.conn.sql("SELECT * FROM test")

    assert len(got.fetchall()) == expected


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            "SELECT * FROM READ_PARQUET(['s3://s3-duckdb-tobiasegelund/2023/01/*'])",
            100,
        ),
    ],
)
def test_DuckSession_save_mode_overwrite(query, expected, session):
    session.sql(query=query).write.mode("overwrite").save("s3://s3-duckdb-tobiasegelund/test")

    got = session.conn.sql("SELECT * FROM READ_PARQUET(['s3://s3-duckdb-tobiasegelund/test/*'])")

    assert len(got.fetchall()) == expected


def test_DuckSession_save_mode_write(session):
    got = False

    query = "SELECT * FROM READ_PARQUET(['s3://s3-duckdb-tobiasegelund/2023/01/*'])"
    try:
        session.sql(query=query).write.save("s3://s3-duckdb-tobiasegelund/test")
    except DatasetExistError as _:
        got = True
    assert got
