import pytest

from duckingit._config import DuckConfig
from duckingit._exceptions import ConfigurationError


# TODO: Write tests for all settings
@pytest.mark.parametrize(
    "name, old_value, new_value",
    [
        ("aws_lambda.MemorySize", 128, 256),
        ("aws_lambda.Timeout", 30, 90),
        ("aws_lambda.FunctionName", "DuckExecutor", "TestFunc"),
        ("aws_lambda.WarmUp", False, True),
    ],
)
def test_DuckConfig_set(name, old_value, new_value):
    configs = DuckConfig()

    assert getattr(configs, name) == old_value
    DuckConfig().set(name, new_value)
    assert getattr(configs, name) == new_value


@pytest.mark.parametrize(
    "name, value",
    [
        ("aws_lambda.FunctioName", "DuckExecutor"),
        ("aws_lambda.MemorySize", 127),
        ("aws_lambda.Timeout", 1),
        ("aws_lambda.WarmUp", 2),
    ],
)
def test_DuckConfig_set_error(name, value):
    got = False
    try:
        DuckConfig().set(name, value)
    except (ConfigurationError, ValueError):
        got = True

    assert got


def test_DuckConfig_set_multiple():
    conf = (
        DuckConfig()
        .set("aws_lambda.FunctionName", "TestFunc")
        .set("aws_lambda.MemorySize", 256)
    )

    assert len(set(conf.services_to_be_updated)) == 1
