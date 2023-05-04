import datetime
import typing as t

from duckingit._planner import Plan, Task, Stage, Stages
from duckingit.integrations import Providers
from duckingit._utils import scan_source_for_files
from duckingit._exceptions import FailedLambdaFunctions

if t.TYPE_CHECKING:
    from duckingit._session import DuckSession


ITERATIONS_TO_CHECK_FAILED = 10
WAIT_TIME_SUCCESS_QUEUE_SECONDS = [2, 2, 4, 6]
WAIT_TIME_FAILURE_QUEUE_SECONDS = 5


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

        self.success_queue = getattr(self.session.conf, "aws_sqs.QueueSuccess")
        self.failure_queue = getattr(self.session.conf, "aws_sqs.QueueFailure")

    def _set_provider(self):
        self.provider = Providers.AWS.klass

    def fetch_cache_metadata(self) -> dict[str, datetime.datetime]:
        return self.session.metadata_cached

    def update_cache_metadata(
        self, execution_stage: Stage, execution_time: datetime.datetime
    ) -> None:
        for task in execution_stage.tasks:
            self.session.metadata_cached[task.subquery_hashed] = execution_time

    def scan_cache_data(self, source: str) -> list[str]:
        return scan_source_for_files(source=source)

    def evaluate_execution_stage(self, execution_stage: Stage, source: str) -> None:
        """Evaluate the execution plan

        Filters cached objects to minimize compute power
        """
        cached_objects = self.scan_cache_data(source=source)
        session_cache_metadata = self.fetch_cache_metadata()

        for step in execution_stage.tasks[:]:
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
                execution_stage.tasks.remove(step)

    def execute_plan(self, execution_plan: Plan, prefix: str, default_prefix: str):
        """Executes the execution plan"""
        context: dict[str, list[str]] = {}
        completed = set()
        queue = set(execution_plan.leaves)

        while queue:
            stage = queue.pop()

            for deb in stage.dependents:
                if deb.stage_type == Stages.CTE:
                    continue
                if deb not in completed:
                    queue.add(deb)

            # CREATE TASKS HERE BASED ON CONTEXT!!
            stage.create_tasks(dependency=context)
            # TODO: Handle multi dependencies
            # context[stage.id] = list(
            #     prefix + "/" + i + ".parquet" for i in stage.output
            # )

            if stage.id == execution_plan.root.id and prefix != "":
                default_prefix = prefix

            context["output"] = list(
                default_prefix + "/" + i + ".parquet" for i in stage.output
            )
            self.evaluate_execution_stage(execution_stage=stage, source=prefix)

            execution_time = datetime.datetime.now()
            if len(stage) > 0:
                request_ids = self.provider.invoke(
                    execution_tasks=stage.tasks, prefix=prefix
                )

                self.check_status_of_invokations(request_ids=request_ids)

            completed.add(stage)
            self.update_cache_metadata(
                execution_stage=stage, execution_time=execution_time
            )

    def check_status_of_invokations(self, request_ids: dict[str, Task]):
        cnt = 0

        while len(request_ids) > 0:
            # Logic to speed up fast queries
            if cnt < len(WAIT_TIME_SUCCESS_QUEUE_SECONDS):
                wait_time = WAIT_TIME_SUCCESS_QUEUE_SECONDS[cnt]
            messages = self.provider.poll_messages_from_queue(
                name=self.success_queue, wait_time_seconds=wait_time
            )

            if len(messages) > 0:
                for message in messages:
                    try:
                        request_ids.pop(message.request_id)
                    except KeyError:
                        continue

                entries = list(message.create_entry_payload() for message in messages)
                self.provider.delete_messages_from_queue(
                    name=self.success_queue, entries=entries
                )

            cnt += 1

            if cnt % ITERATIONS_TO_CHECK_FAILED == 0:
                messages = self.provider.poll_messages_from_queue(
                    name=self.failure_queue,
                    wait_time_seconds=WAIT_TIME_FAILURE_QUEUE_SECONDS,
                )

                if len(messages) > 0:
                    raise FailedLambdaFunctions(f"{messages} failed")

    # def show(self):
    #     # Select only X parquet files?
    #     pass
