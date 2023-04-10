import pytest

from duckingit._provider import AWS


@pytest.fixture
def mockAWS(MockAWS):
    yield MockAWS(function_name="TestFunc")


# @pytest.mark.parametrize("method", [("invoke_sync"), ("invoke_async")])
# def test_invoke(method, mockAWS):
#     expected = {
#         "data": [(100, "John", "Doe"), (101, "Eric", "Doe"), (102, "Maria", "Doe")],
#         "dtypes": ["BIGINT", "VARCHAR", "VARCHAR"],
#         "columns": ["id", "first_name", "last_name"],
#     }

#     queries = ["SELECT * FROM TEST", "SELECT * FROM TEST2"]
#     method = getattr(mockAWS, method)

#     gots = method(queries=queries)

#     for got in gots:
#         assert got.get("data") == expected.get("data")
#         assert got.get("dtypes") == expected.get("dtypes")
#         assert got.get("columns") == expected.get("columns")
