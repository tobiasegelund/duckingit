from duckingit._config import DuckConfig


def test_DuckConfig():
    got = False
    try:
        conf = DuckConfig().set("aws_lambda.MemorySize", 128)
        got = True
    except Exception:
        got = False

    assert got
