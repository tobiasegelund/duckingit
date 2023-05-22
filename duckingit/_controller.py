import datetime
import typing as t

from duckingit._exceptions import FailedLambdaFunctions
from duckingit._planner import Plan, Stage, Task
from duckingit._utils import scan_source_for_files
from duckingit.providers import Providers

if t.TYPE_CHECKING:
    from duckingit._session import DuckSession


# TODO: define heuristics for this
ITERATIONS_TO_CHECK_FAILED = 3
WAIT_TIME_SUCCESS_QUEUE_SECONDS = [3] * ITERATIONS_TO_CHECK_FAILED
WAIT_TIME_FAILURE_QUEUE_SECONDS = 1


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
        self.cache_expiration_time = getattr(session.conf, "session.cache_expiration_time")

        self.success_queue = getattr(self.session.conf, "aws_sqs.QueueSuccess")
        self.failure_queue = getattr(self.session.conf, "aws_sqs.QueueFailure")
        self.verbose = getattr(self.session.conf, "session.verbose")

    def _set_provider(self):
        self.provider = Providers.get_or_raise("aws")

    def fetch_cache_metadata(self) -> dict[str, datetime.datetime]:
        return self.session.metadata_cached

    def update_cache_metadata(
        self, execution_stage: Stage, execution_time: datetime.datetime
    ) -> None:
        for task in execution_stage.tasks:
            self.session.metadata_cached[task.subquery_hashed] = execution_time

    def scan_cache_data(self, prefix: str) -> list[str]:
        return scan_source_for_files(source=prefix)

    def evaluate_execution_stage(self, execution_stage: Stage, prefix: str) -> None:
        """Evaluate the execution plan

        Filters cached objects to minimize compute power
        """
        cached_objects = self.scan_cache_data(prefix=prefix)
        session_cache_metadata = self.fetch_cache_metadata()

        for step in list(execution_stage.tasks):
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

    def execute_stage(
        self,
        stage: Stage,
        dag: dict[Stage, t.Set[Stage]],
        context: dict[str, list[str]],
        completed: t.Set[Stage],
        root_id: str,
        prefix: str,
        default_prefix: str,
    ):
        for dep in dag[stage]:
            if dep not in completed:
                self.execute_stage(
                    stage=dep,
                    dag=dag,
                    context=context,
                    completed=completed,
                    root_id=root_id,
                    prefix=prefix,
                    default_prefix=default_prefix,
                )

        stage_deps = {}
        for dep in stage.dependencies:
            if dep.id in context:
                stage_deps[dep.id] = context[dep.id]

        stage.create_tasks(dependencies=context)
        if self.verbose:
            print(f"RUNNING STAGE: [{stage}]")

        if stage.id == root_id and prefix != "":
            default_prefix = prefix

        context[stage.id] = [f"{default_prefix}/{i}.parquet" for i in stage.output]
        # self.evaluate_execution_stage(execution_stage=stage, prefix=default_prefix)

        execution_time = datetime.datetime.now()
        if len(stage.tasks) > 0:
            request_ids = self.provider.lambda_.invoke(
                execution_tasks=stage.tasks, prefix=default_prefix
            )

            self.check_status_of_invokations(request_ids=request_ids)

        completed.add(stage)
        self.update_cache_metadata(execution_stage=stage, execution_time=execution_time)

    def execute_plan(self, execution_plan: Plan, prefix: str, default_prefix: str):
        """Executes the execution plan"""
        completed: t.Set[Stage] = set()
        dag = execution_plan.dag
        context: dict[str, list[str]] = {}

        for stage in dag:
            if stage not in completed:
                self.execute_stage(
                    stage=stage,
                    dag=dag,
                    context=context,
                    completed=completed,
                    root_id=execution_plan.root.id,
                    prefix=prefix,
                    default_prefix=default_prefix,
                )

    def check_status_of_invokations(self, request_ids: dict[str, Task]):
        cnt = 0

        total_tasks = len(request_ids)
        while len(request_ids) > 0:
            # Logic to speed up fast queries
            if cnt < len(WAIT_TIME_SUCCESS_QUEUE_SECONDS):
                wait_time = WAIT_TIME_SUCCESS_QUEUE_SECONDS[cnt]
            messages = self.provider.sqs.poll_messages_from_queue(
                name=self.success_queue, wait_time_seconds=wait_time
            )

            if len(messages) > 0:
                for message in messages:
                    try:
                        request_ids.pop(message.request_id)
                    except KeyError:
                        continue

                entries = list(message.create_entry_payload() for message in messages)
                self.provider.sqs.delete_messages_from_queue(
                    name=self.success_queue, entries=entries
                )

            if self.verbose:
                print(f"\tTASKS COMPLETED: {total_tasks - len(request_ids)}/{total_tasks}")

            cnt += 1

            if cnt % ITERATIONS_TO_CHECK_FAILED == 0:
                messages = self.provider.sqs.poll_messages_from_queue(
                    name=self.failure_queue,
                    wait_time_seconds=WAIT_TIME_FAILURE_QUEUE_SECONDS,
                )

                if len(messages) > 0:
                    self.provider.sqs.purge_queue(self.failure_queue)  # clean up
                    raise FailedLambdaFunctions(f"{messages}")

    # def show(self):
    #     # Select only X parquet files?
    #     pass
