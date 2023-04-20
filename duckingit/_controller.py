import datetime
import time
import typing as t

from duckingit._planner import Plan, Step
from duckingit.integrations import Providers
from duckingit._utils import scan_source_for_files
from duckingit._exceptions import FailedLambdaFunctions

if t.TYPE_CHECKING:
    from duckingit._session import DuckSession


SECONDS_TO_CHECK_FAILED = 2
SECONDS_TO_FAIL_OPERATION = 900  # 15 minutes


class Controller:
    """The purpose of the controller is to control the invokations of
    serverless functions, e.g. Lambda functions.

    It invokes and collects the data, as well as concatenate it altogether before it's
    delivered to the user.

    - Only select a subset of partitions (minimize throughput)
            Can be based on number of rows or byte size
    """

    def __init__(self, session: "DuckSession") -> None:
        self.session = session

        self._set_provider()
        self.cache_expiration_time = getattr(
            session.conf, "session.cache_expiration_time"
        )

    def _set_provider(self):
        self.provider = Providers.AWS.klass

    def fetch_cache_metadata(self) -> dict[str, datetime.datetime]:
        return self.session.metadata_cached

    def update_cache_metadata(
        self, execution_plan: Plan, execution_time: datetime.datetime
    ) -> None:
        for step in execution_plan.execution_steps:
            self.session.metadata_cached[step.subquery_hashed] = execution_time

    def scan_cache_data(self, source: str) -> list[str]:
        return scan_source_for_files(source=source)

    def evaluate_execution_plan(self, execution_plan: Plan, source: str):
        """Evaluate the execution plan

        For example filter cached objects to minimize compute power
        """
        cached_objects = self.scan_cache_data(source=source)
        session_cache_metadata = self.fetch_cache_metadata()

        for idx, step in enumerate(execution_plan.execution_steps):
            last_executed = session_cache_metadata.get(step.subquery_hashed, None)

            if last_executed is None:
                continue

            last_executed_seconds = (datetime.datetime.now() - last_executed).seconds
            last_executed_minutes = int(last_executed_seconds / 60)

            # The order of evaluations matters
            if (
                last_executed_minutes < self.cache_expiration_time
                and step.subquery_hashed in cached_objects
            ):
                execution_plan.execution_steps.pop(idx)

    def execute_plan(self, execution_plan: Plan, prefix: str):
        """Executes the execution plan"""

        self.evaluate_execution_plan(execution_plan=execution_plan, source=prefix)

        execution_time = datetime.datetime.now()
        if len(execution_plan.execution_steps) > 0:
            request_ids = self.provider.invoke(
                execution_steps=execution_plan.execution_steps, prefix=prefix
            )

            self.check_status_of_invokations(request_ids=request_ids)

        self.update_cache_metadata(
            execution_plan=execution_plan, execution_time=execution_time
        )

    def check_status_of_invokations(self, request_ids: dict[str, Step]):
        while len(request_ids) > 0:
            success_ids = self.provider.poll_messages_from_success_queue()

            for _id in success_ids:
                request_ids.pop(_id)

            if time.time() % SECONDS_TO_CHECK_FAILED == 0:
                failure_ids = self.provider.poll_messages_from_failure_queue()

                if len(failure_ids) > 0:
                    # failed_items = list(request_ids.get(i) for i in failure_ids)
                    FailedLambdaFunctions()

            if time.time() % SECONDS_TO_FAIL_OPERATION == 0:
                raise FailedLambdaFunctions()

    # def show(self):
    #     # Select only X parquet files?
    #     pass
