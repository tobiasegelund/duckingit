import pytest

from duckingit.integrations.aws import AWS
from duckingit._session import DuckSession
from duckingit._controller import LocalController
from duckingit._parser import Query


class _MockAWS(AWS):
    def _invoke_lambda_sync(self, request_payload: str) -> dict:
        return {
            "data": [(100, "John", "Doe"), (101, "Eric", "Doe"), (102, "Maria", "Doe")],
            "dtypes": ["BIGINT", "VARCHAR", "VARCHAR"],
            "columns": ["id", "first_name", "last_name"],
        }


class _MockLocalController(LocalController):
    _mock_tmp_name = "tmp1"

    def _create_tmp_table_name(self) -> str:
        return self._mock_tmp_name

    def _create_tmp_table(self, table_name: str, prefix: str) -> None:
        self.conn.sql(
            f"CREATE TEMP TABLE {self._mock_tmp_name} AS (SELECT * FROM 'tests/unit/data/test_data.parquet')"
        )


class _MockDuckSession(DuckSession):
    def _set_controller(self):
        return _MockLocalController(
            conn=self._conn, provider=_MockAWS(function_name=self._function_name)
        )

    def _scan_bucket(self, query: Query) -> list[str]:
        return ["s3://BUCKET_NAME/2023/03/*"]


@pytest.fixture
def MockAWS():
    yield _MockAWS


@pytest.fixture
def MockDuckSession():
    yield _MockDuckSession


@pytest.fixture
def MockLocalController():
    yield _MockLocalController
