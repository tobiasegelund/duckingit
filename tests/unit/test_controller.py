import pytest
import duckdb

from duckingit._controller import LocalController
from duckingit._provider import AWS


@pytest.fixture
def controller(MockAWS, MockLocalController):
    conn = duckdb.connect(":memory:")
    yield MockLocalController(conn=conn, provider=MockAWS(function_name="TestFunc"))
