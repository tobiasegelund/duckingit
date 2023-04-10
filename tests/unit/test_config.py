import pytest

from duckingit._config import DuckConfig


@pytest.mark.parametrize(
    "timeout, memory_size", [(1, 128), (901, 128), (3, 64), (3, 150000)]
)
def test_config_errors(timeout, memory_size):
    got = False

    try:
        DuckConfig().timeout(timeout=timeout).memory_size(memory_size=memory_size)

    except ValueError as e:
        got = True

    assert got
