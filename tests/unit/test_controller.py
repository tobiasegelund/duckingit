import pytest
import duckdb

from duckingit._controller import LocalController
from duckingit._provider import MockProvider


@pytest.fixture
def controller():
    conn = duckdb.connect(":memory:")
    yield LocalController(conn=conn, provider=MockProvider())
