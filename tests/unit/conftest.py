import pytest
import duckdb

from duckingit._provider import AWS
from duckingit._planner import Planner
from duckingit._session import DuckSession


class _MockAWS(AWS):
    def _invoke_lambda_sync(self, request_payload: str) -> dict:
        return {
            "data": [(100, "John", "Doe"), (101, "Eric", "Doe"), (102, "Maria", "Doe")],
            "dtypes": ["BIGINT", "VARCHAR", "VARCHAR"],
            "columns": ["id", "first_name", "last_name"],
        }


class _MockPlanner(Planner):
    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self.conn = conn

    def scan_bucket(self, key: str) -> list[str]:
        return [
            "s3://<BUCKET_NAME>/2023/01/*",
            "s3://<BUCKET_NAME>/2023/02/*",
            "s3://<BUCKET_NAME>/2023/03/*",
        ]


class _MockDuckSession(DuckSession):
    pass


@pytest.fixture
def MockAWS():
    yield _MockAWS


@pytest.fixture
def MockPlanner():
    yield _MockPlanner


@pytest.fixture
def MockDuckSession():
    yield _MockDuckSession
