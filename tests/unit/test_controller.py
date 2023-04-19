import pytest
import datetime

from unittest.mock import MagicMock


@pytest.mark.parametrize(
    "cached_objects, expiration_time, expected",
    [
        ({"39b3103cb8bf95523be3bdc1faa7e67d": datetime.datetime.now()}, 15, 2),
        ({"39b3103cb8bf95523be3bdc1faa7e67d": datetime.datetime.now()}, 0, 3),
        ({}, 60, 3),
    ],
)
def test_evaluate_execution_plan(
    cached_objects,
    expiration_time,
    expected,
    MockController,
    MockDuckSession,
    MockPlan,
):
    session = MockDuckSession()
    session.conf.cache_expiration_time(expiration_time)
    controller = MockController(session=session)

    controller.fetch_cache_metadata = MagicMock(return_value=cached_objects)
    controller.evaluate_execution_plan(MockPlan, source="SourceIsMocked")

    assert len(MockPlan.execution_steps) == expected


@pytest.mark.parametrize(
    "cached_objects",
    [
        ({}),
        ({"39b3103cb8bf95523be3bdc1faa7e67d": datetime.datetime.now()}),
    ],
)
def test_evaluate_execution_plan_no_cached_objects(
    cached_objects,
    MockController,
    MockDuckSession,
    MockPlan,
):
    session = MockDuckSession()
    controller = MockController(session=session)

    controller.fetch_cache_metadata = MagicMock(return_value=cached_objects)
    controller.scan_cache_data = MagicMock(return_value=[])
    controller.evaluate_execution_plan(MockPlan, source="SourceIsMocked")

    assert len(MockPlan.execution_steps) == 3


def test_execute_plan(
    MockController,
    MockDuckSession,
    MockPlan,
):
    session = MockDuckSession()
    assert len(session.metadata_cached) == 0

    controller = MockController(session=session)
    controller.execute_plan(MockPlan, prefix="SourceIsMocked")

    assert len(session.metadata_cached) == 3
