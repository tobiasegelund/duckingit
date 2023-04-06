import duckdb


def lambda_handler(event, context):
    con = duckdb.connect(
        database=":memory:",
        read_only=False,
        # config={"allow_unsigned_extensions": "true"},
    )

    con.execute("SET home_directory='/opt/python'; LOAD httpfs;")
    # con.execute("SET home_directory='/tmp'; INSTALL httpfs; LOAD httpfs;")

    # con.execute(
    #     """
    # SET enable_http_metadata_cache=true;
    # SET enable_object_cache=true;
    # """
    # )

    payload = event["body"]

    data = con.sql(payload)

    return {
        "columns": data.columns,
        "dtypes": data.dtypes,
        "data": data.fetchall(),
    }
