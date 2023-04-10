import duckdb
import json

con = duckdb.connect(
    database=":memory:",
    read_only=False,
    # config={"allow_unsigned_extensions": "true"},
)

con.execute("SET home_directory='/opt/python'; LOAD httpfs;")
# con.execute(
#     """
# SET enable_http_metadata_cache=true;
# SET enable_object_cache=true;
# """
# )


def lambda_handler(event, context):
    key = event["key"]  # key to write to in S3
    payload = event["query"]

    try:
        con.sql(
            f"""
            COPY ({payload}) TO '{key}'
            """
        )
        return {"statusCode": 200, "url": ""}
    except Exception as e:
        return {
            "statusCode": 400,
            "errorMessage": json.dumps(e),
        }
