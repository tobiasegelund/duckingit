import pytest

from duckingit import DuckSession


@pytest.fixture
def test_function_name_aws():
    yield "TestFunc"


@pytest.fixture
def session():
    yield DuckSession(function_name="DuckExecutor")
