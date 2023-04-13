from duckingit._config import DuckConfig


def test_DuckConfig(test_function_name_aws):
    got = False
    try:
        DuckConfig(function_name=test_function_name_aws).memory_size(128).timeout(
            30
        ).warm_up().update()
        got = True
    except Exception:
        got = False

    assert got
