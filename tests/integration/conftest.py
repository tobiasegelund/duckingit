import pytest

from duckingit import DuckSession


@pytest.fixture
def session():
    yield DuckSession(function_name="DuckDBFinal")
