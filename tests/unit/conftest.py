import pytest
import duckdb

from duckingit.integrations.aws import AWS
from duckingit._session import DuckSession
from duckingit._controller import LocalController
from duckingit._dataset import Dataset


class _MockAWS(AWS):
    def _invoke_lambda_sync(self, request_payload: str) -> dict:
        return {
            "data": [(100, "John", "Doe"), (101, "Eric", "Doe"), (102, "Maria", "Doe")],
            "dtypes": ["BIGINT", "VARCHAR", "VARCHAR"],
            "columns": ["id", "first_name", "last_name"],
        }


class _MockDataset(Dataset):
    _mock_tmp_name = "tmp1"
    _provider = _MockAWS(function_name="TestFunc")

    def _create_tmp_table(self, table_name: str) -> None:
        self._session.conn.sql(
            f"CREATE TEMP TABLE {self._mock_tmp_name} AS (SELECT * FROM 'tests/unit/data/test_data.parquet')"
        )


class _MockLocalController(LocalController):
    def _scan_bucket(self, bucket: str) -> list[str]:
        return ["s3://BUCKET_NAME/2023/03/*"]


class _MockDuckSession(DuckSession):
    def _set_controller(self):
        self._controller = _MockLocalController(session=self)


@pytest.fixture
def MockAWS():
    yield _MockAWS


@pytest.fixture
def MockDuckSession():
    yield _MockDuckSession


@pytest.fixture
def MockLocalController():
    yield _MockLocalController
