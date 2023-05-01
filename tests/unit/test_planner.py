import pytest

from duckingit._exceptions import WrongInvokationType
from duckingit._parser import Query
from duckingit._planner import Plan, Task


# def test_execution_plan():
#     query = Query.parse("SELECT * FROM scan_parquet(['s3://BUCKET_NAME/2023/*'])")
#     query.list_of_prefixes = [
#         "s3://BUCKET_NAME/2023/01/*",
#         "s3://BUCKET_NAME/2023/02/*",
#         "s3://BUCKET_NAME/2023/03/*",
#     ]
#     got = Plan.create_from_query(query=query, invokations=1)


@pytest.mark.parametrize(
    "query, invokations, expected",
    [
        (
            "SELECT * FROM scan_parquet(['s3://BUCKET_NAME/2023/*'])",
            1,
            [
                "SELECT * FROM READ_PARQUET(['s3://BUCKET_NAME/2023/01/*', 's3://BUCKET_NAME/2023/02/*', 's3://BUCKET_NAME/2023/03/*'])"
            ],
        ),
        (
            "SELECT * FROM scan_parquet(['s3://BUCKET_NAME/2023/*'])",
            2,
            [
                "SELECT * FROM READ_PARQUET(['s3://BUCKET_NAME/2023/01/*', 's3://BUCKET_NAME/2023/02/*'])",
                "SELECT * FROM READ_PARQUET(['s3://BUCKET_NAME/2023/03/*'])",
            ],
        ),
        (
            "SELECT * FROM scan_parquet(['s3://BUCKET_NAME/2023/*'])",
            3,
            [
                "SELECT * FROM READ_PARQUET(['s3://BUCKET_NAME/2023/01/*'])",
                "SELECT * FROM READ_PARQUET(['s3://BUCKET_NAME/2023/02/*'])",
                "SELECT * FROM READ_PARQUET(['s3://BUCKET_NAME/2023/03/*'])",
            ],
        ),
        (
            "SELECT * FROM scan_parquet(['s3://BUCKET_NAME/2023/*'])",
            4,
            [
                "SELECT * FROM READ_PARQUET(['s3://BUCKET_NAME/2023/01/*'])",
                "SELECT * FROM READ_PARQUET(['s3://BUCKET_NAME/2023/02/*'])",
                "SELECT * FROM READ_PARQUET(['s3://BUCKET_NAME/2023/03/*'])",
            ],
        ),
        (
            "SELECT * FROM scan_parquet(['s3://BUCKET_NAME/2023/*'])",
            "auto",
            [
                "SELECT * FROM READ_PARQUET(['s3://BUCKET_NAME/2023/01/*'])",
                "SELECT * FROM READ_PARQUET(['s3://BUCKET_NAME/2023/02/*'])",
                "SELECT * FROM READ_PARQUET(['s3://BUCKET_NAME/2023/03/*'])",
            ],
        ),
        (
            "SELECT * FROM READ_JSON_AUTO(['s3://BUCKET_NAME/2023/*'])",
            "auto",
            [
                "SELECT * FROM READ_JSON_AUTO(['s3://BUCKET_NAME/2023/01/*'])",
                "SELECT * FROM READ_JSON_AUTO(['s3://BUCKET_NAME/2023/02/*'])",
                "SELECT * FROM READ_JSON_AUTO(['s3://BUCKET_NAME/2023/03/*'])",
            ],
        ),
        (
            "SELECT * FROM READ_CSV_AUTO(['s3://BUCKET_NAME/2023/*'])",
            "auto",
            [
                "SELECT * FROM READ_CSV_AUTO(['s3://BUCKET_NAME/2023/01/*'])",
                "SELECT * FROM READ_CSV_AUTO(['s3://BUCKET_NAME/2023/02/*'])",
                "SELECT * FROM READ_CSV_AUTO(['s3://BUCKET_NAME/2023/03/*'])",
            ],
        ),
    ],
)
def test_execution_steps(query, invokations, expected):
    query = Query.parse(query)
    query._list_of_prefixes = [
        "s3://BUCKET_NAME/2023/01/*",
        "s3://BUCKET_NAME/2023/02/*",
        "s3://BUCKET_NAME/2023/03/*",
    ]
    plan = Plan.create_from_query(query=query, invokations=invokations)

    got = plan.execution_steps

    assert list(i.subquery for i in got) == expected


def test_plan_error():
    got = False

    query = "SELECT * FROM scan_parquet(['s3://BUCKET_NAME/2023/*'])"
    query = Query.parse(query)
    try:
        _ = Plan.create_from_query(query=query, invokations="1")
    except WrongInvokationType:
        got = True

    assert got
