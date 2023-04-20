import pytest

from duckingit.integrations.aws import AWS
from duckingit._session import DuckSession
from duckingit._controller import Controller
from duckingit._dataset import Dataset
from duckingit._config import DuckConfig
from duckingit._planner import Plan
from duckingit._parser import Query
from duckingit._utils import create_hash_string


class _MockAWS(AWS):
    def _invoke_lambda_sync(self, request_payload: str) -> dict:
        return {
            "data": [(100, "John", "Doe"), (101, "Eric", "Doe"), (102, "Maria", "Doe")],
            "dtypes": ["BIGINT", "VARCHAR", "VARCHAR"],
            "columns": ["id", "first_name", "last_name"],
        }


class _MockController(Controller):
    def _set_provider(self):
        self.provider = _MockAWS()

    def scan_cache_data(self, source: str) -> list[str]:
        return list(
            create_hash_string(subquery, algorithm="md5")
            for subquery in [
                "SELECT * FROM READ_PARQUET(['s3://BUCKET_NAME/2023/01/*'])",
                "SELECT * FROM READ_PARQUET(['s3://BUCKET_NAME/2023/02/*'])",
                "SELECT * FROM READ_PARQUET(['s3://BUCKET_NAME/2023/03/*'])",
            ]
        )


class _MockDataset(Dataset):
    _mock_tmp_name = "tmp1"

    def _create_tmp_table(self, table_name: str) -> None:
        self._session.conn.sql(
            f"CREATE TEMP TABLE {self._mock_tmp_name} AS (SELECT * FROM 'tests/unit/data/test_data.parquet')"
        )

    def _set_controller(self):
        self._controller = _MockController(session=self._session)


class _MockDuckConfig(DuckConfig):
    pass


class _MockDuckSession(DuckSession):
    def _set_conf(self) -> None:
        self._conf = _MockDuckConfig()


@pytest.fixture()
def MockQuery():
    query = Query.parse("SELECT * FROM READ_PARQUET(['s3://BUCKET_NAME/2023/*'])")
    query._list_of_prefixes = [
        "s3://BUCKET_NAME/2023/01/*",
        "s3://BUCKET_NAME/2023/02/*",
        "s3://BUCKET_NAME/2023/03/*",
    ]
    yield query


@pytest.fixture
def MockPlan(MockQuery):
    plan = Plan.create_from_query(MockQuery, invokations="auto")
    yield plan


# @pytest.fixture
# def MockAWS():
#     yield _MockAWS


@pytest.fixture
def MockDuckSession():
    yield _MockDuckSession


@pytest.fixture
def MockController():
    yield _MockController
