import duckdb

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
    try:
        if event["WARMUP"] == 1:
            return
    except KeyError:
        pass

    key = event["key"]  # key to S3
    query = event["query"]
    con.sql("COPY ({query}) TO '{key}' (FORMAT 'PARQUET')".format(key=key, query=query))
    return {"statusCode": 200}
