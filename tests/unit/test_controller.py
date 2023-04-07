import pytest
import duckdb

from duckingit._controller import LocalController
from duckingit._provider import AWS


@pytest.fixture
def controller(MockAWS):
    conn = duckdb.connect(":memory:")
    yield LocalController(conn=conn, provider=MockAWS(function_name="TestFunc"))
