import pytest
import duckdb

from duckingit._provider import AWS
from duckingit._planner import Planner
from duckingit._session import DuckSession
from duckingit._controller import LocalController


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

    def _scan_bucket(self, prefix: str) -> list[str]:
        return [
            "s3://<BUCKET_NAME>/2023/01/*",
            "s3://<BUCKET_NAME>/2023/02/*",
            "s3://<BUCKET_NAME>/2023/03/*",
        ]


class _MockLocalController(LocalController):
    _mock_tmp_name = "tmp1"

    def _create_tmp_table_name(self) -> str:
        return self._mock_tmp_name

    def _create_tmp_table(self, table_name: str, prefix: str) -> None:
        self.conn.sql(
            f"CREATE TEMP TABLE {self._mock_tmp_name} AS (SELECT * FROM 'tests/unit/data/test_data.parquet')"
        )


class _MockDuckSession(DuckSession):
    def _set_planner(self) -> Planner:
        return _MockPlanner(conn=self._conn)

    def _set_controller(self):
        return _MockLocalController(
            conn=self._conn, provider=_MockAWS(function_name=self._function_name)
        )


@pytest.fixture
def MockAWS():
    yield _MockAWS


@pytest.fixture
def MockPlanner():
    yield _MockPlanner


@pytest.fixture
def MockDuckSession():
    yield _MockDuckSession


@pytest.fixture
def MockLocalController():
    yield _MockLocalController
