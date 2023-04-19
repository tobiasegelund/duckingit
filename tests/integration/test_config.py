from duckingit._config import DuckConfig


def test_DuckConfig():
    got = False
    try:
        DuckConfig(function_name="TestFunc").memory_size(128).timeout(
            30
        ).warm_up().update()
        got = True
    except Exception:
        got = False

    assert got
