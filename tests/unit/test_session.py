import pytest

from duckingit._exceptions import ParserError


@pytest.fixture
def session(MockDuckSession):
    yield MockDuckSession(function_name="TestFunc")


# def test_DuckSession_execute(session):
#     expected = 3
#     conn = session.execute(
#         query="SELECT * FROM scan_parquet(['s3://BUCKET_NAME/2023/03/*'])"
#     )

#     got = len(conn.fetchall())

#     assert got == expected


@pytest.mark.parametrize(
    "query",
    [
        ("SELECT * FROM scan_parquet(['s3://BUCKET_NAME/2023/03/*']"),
        ("SELECT * FROM scan_parquet([s3://BUCKET_NAME/2023/03/*])"),
    ],
)
def test_DuckSession_query_error(query, session):
    got = False

    try:
        _ = session.execute(query=query)
    except ParserError:
        got = True

    assert got
