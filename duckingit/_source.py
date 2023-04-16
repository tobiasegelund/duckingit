import typing as t

import duckdb

from duckingit._utils import flatten_list
from duckingit._parser import Query
from duckingit._planner import Plan
from duckingit._dataset import Dataset

if t.TYPE_CHECKING:
    from duckingit._session import DuckSession


class DataSource:
    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        """
        TODO:
            - Manage the mapping of hash values, prefix, pathes to datasets
            - Bucket name? Provider?
        """
        self.conn = conn

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

    def create_dataset(
        self, query: str, invokations: int | str, session: "DuckSession"
    ):
        query_parsed: Query = Query.parse(query)
        query_parsed.list_of_prefixes = self._scan_bucket(bucket=query_parsed.source)

        execution_plan = Plan.create_from_query(
            query=query_parsed, invokations=invokations
        )

        return Dataset(
            conn=self.conn,
            query=query_parsed,
            execution_plan=execution_plan,
            session=session,
        )
