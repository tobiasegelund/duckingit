import pytest

from duckingit._config import DuckConfig
from duckingit._exceptions import ConfigurationError
from duckingit.integrations import Providers


def test_DuckConfig_set_multiple():
    conf = (
        DuckConfig()
        .set("aws_lambda.FunctionName", "DuckExecutor")
        .set("aws_lambda.MemorySize", 128)
    )

    assert len(set(conf.services_to_be_updated)) == 1


# TODO: Write tests for all settings
@pytest.mark.parametrize(
    "name, old_value, new_value",
    [
        ("aws_lambda.MemorySize", 128, 256),
        ("aws_lambda.Timeout", 30, 90),
        ("aws_lambda.FunctionName", "DuckExecutor", "TestFunc"),
        ("aws_lambda.WarmUp", False, True),
        ("aws_sqs.QueueSuccess", "DuckSuccess", "TestSuccess"),
        ("aws_sqs.QueueFailure", "DuckFailure", "TestFailure"),
        ("aws_sqs.MaxNumberOfMessages", 10, 9),
        ("aws_sqs.VisibilityTimeout", 5, 4),
        ("aws_sqs.WaitTimeSeconds", 5, 4),
        ("aws_sqs.DelaySeconds", 0, 1),
        ("aws_sqs.MaximumMessageSize", 2056, 2057),
        ("aws_sqs.MessageRetentionPeriod", 900, 1000),
        ("session.cache_expiration_time", 15, 14),
        ("session.max_invokations", "auto", 15),
        ("session.provider", Providers.AWS, Providers.AWS),
        ("session.verbose", False, True),
        ("duckdb.database", ":memory:", ":memory:"),
        ("duckdb.read_only", False, False),
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
