import duckdb

from duckingit._utils import flatten_list
from duckingit._parser import Query
from duckingit._controller import Controller
from duckingit._planner import Plan


class DataSource:
    _CACHE_PREFIX = ".cache/duckingit"

    def __init__(self, conn: duckdb.DuckDBPyConnection, controller: Controller) -> None:
        """
        TODO:
            - Manage the mapping of hash values, prefix, pathes to datasets
            - Bucket name? Provider?
        """
        self.conn = conn
        self.controller = controller

    def _scan_bucket(self, bucket: str) -> list[str]:
        """Scans the URL for files, e.g. a S3 bucket

        Args:
            bucket, str: The bucket to scan, e.g. s3://BUCKET_NAME/
            conn, duckdb.DuckDBPyConnection: A DuckDB connection
        """

        # TODO: Count the number of files / size in each prefix to divide the workload better
        glob_query = f"""
            SELECT DISTINCT
                CONCAT(REGEXP_REPLACE(file, '/[^/]+$', ''), '/*') AS prefix
            FROM GLOB({bucket})
            """

        try:
            prefixes = self.conn.sql(glob_query).fetchall()

        except RuntimeError:
            raise ValueError(
                "Please validate that the FROM statement in the query is correct."
            )

        return flatten_list(prefixes)

    def _create_prefix(self, query: Query, write_to: str | None) -> str:
        if write_to is not None:
            if write_to[-1] != "/":
                return write_to
            return write_to[:-1]
        return f"{query.bucket}/{self._CACHE_PREFIX}/{query.hashed}"

    def create_dataset(self, query: str, invokations: int | str, write_to: str | None):
        query_parsed: Query = Query.parse(query)
        query_parsed.list_of_prefixes = self._scan_bucket(bucket=query_parsed._source)

        execution_plan = Plan.create_from_query(
            query=query_parsed, invokations=invokations
        )

        prefix = self._create_prefix(query=query_parsed, write_to=write_to)
        duckdb_obj, table_name = self.controller.execute(
            execution_plan=execution_plan, prefix=prefix
        )

        return duckdb_obj, table_name
