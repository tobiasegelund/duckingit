import typing as t

import duckdb

from duckingit._planner import Plan
from duckingit._utils import flatten_list
from duckingit._parser import Query
from duckingit._dataset import Dataset

if t.TYPE_CHECKING:
    from duckingit._session import DuckSession


class Controller:
    def execute(
        self, execution_plan: Plan, prefix: str
    ) -> tuple[duckdb.DuckDBPyRelation, str]:
        raise NotImplementedError()


class LocalController(Controller):
    """The purpose of the controller is to control the invokations of
    serverless functions, e.g. Lambda functions.

    It invokes and collects the data, as well as concatenate it altogether before it's
    delivered to the user.

    TODO:
        - Incorporate cache functionality to minimize compute power.
        - Copy from cache?
        - Only select a subset of partitions (minimize throughput)
            Can be based on number of rows or byte size
    """

    def __init__(self, session: "DuckSession") -> None:
        self._session = session

    def scan_bucket_for_prefixes(self, bucket: str) -> list[str]:
        """Scans the URL for files, e.g. a S3 bucket

        Args:
            bucket, str: The bucket to scan, e.g. s3://BUCKET_NAME/
        """

        # TODO: Count the number of files / size in each prefix to divide the workload better
        glob_query = f"""
            SELECT DISTINCT
                CONCAT(REGEXP_REPLACE(file, '/[^/]+$', ''), '/*') AS prefix
            FROM GLOB({bucket})
            """

        try:
            prefixes = self._session.conn.sql(glob_query).fetchall()

        except RuntimeError:
            raise ValueError(
                "Please validate that the FROM statement in the query is correct."
            )

        return flatten_list(prefixes)

    def evaluate_execution_plan(self):
        pass

    def create_dataset(self, query: str, invokations: int | str):
        parsed_query: Query = Query.parse(query)
        parsed_query.list_of_prefixes = self.scan_bucket_for_prefixes(
            bucket=parsed_query.source
        )

        execution_plan = Plan.create_from_query(
            query=parsed_query, invokations=invokations
        )

        return Dataset(
            query=parsed_query,
            execution_plan=execution_plan,
            session=self._session,
        )


# class RemoteController(Controller):
#     """Class to communicate with controller running as a serverless function"""

#     pass
