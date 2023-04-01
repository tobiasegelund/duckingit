import duckdb

home_directory = "/tmp/build/python"

# Install HTTPFS DuckDB extension
db = duckdb.connect()
db.execute(f"SET home_directory='{home_directory}'; INSTALL httpfs;")
