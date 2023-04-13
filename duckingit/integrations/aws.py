import json
import asyncio
from typing import Literal

import boto3

from .base import Provider
from duckingit._exceptions import MisConfigurationError
from duckingit._planner import Step


class AWS(Provider):
    def __init__(
        self, function_name: str, invokation_type: Literal["sync", "async"] = "async"
    ) -> None:
        if invokation_type not in ["sync", "async"]:
            raise ValueError(
                f"{invokation_type} isn't an option. Only 'sync' or 'async' as \
invokation_type parameter."
            )
        self.function_name = function_name
        self.invokation_type = invokation_type

        self._client = boto3.client("lambda")

    def warm_up(self) -> None:
        """Method to avoid cold starts"""
        _ = self._client.invoke(
            FunctionName=self.function_name,
            Payload=json.dumps({"WARMUP": 1}),
            InvocationType="RequestResponse",
        )

    def invoke_sync(self, execution_steps: list[Step], prefix: str) -> None:
        for step in execution_steps:
            # TODO: Allow other naming options than just hashed
            key = prefix + "/" + step.subquery_hashed + ".parquet"
            request_payload = json.dumps({"query": step.subquery, "key": key})
            _ = self._invoke_lambda_sync(request_payload=request_payload)

    def invoke_async(self, execution_steps: list[Step], prefix: str):
        asyncio.run(self._invoke_async(execution_steps=execution_steps, prefix=prefix))

    async def _invoke_async(self, execution_steps: list[Step], prefix: str) -> None:
        tasks = []
        for step in execution_steps:
            key = prefix + "/" + step.subquery_hashed + ".parquet"
            request_payload = json.dumps({"query": step.subquery, "key": key})

            task = asyncio.create_task(self._invoke_lambda_async(request_payload))
            tasks.append(task)
        tasks_to_run = await asyncio.gather(*tasks)
        return tasks_to_run

    def _invoke_lambda_sync(self, request_payload: str) -> None:
        resp = self._client.invoke(
            FunctionName=self.function_name,
            Payload=request_payload,
            InvocationType="RequestResponse",  # Event
        )

        resp_payload = json.loads(resp["Payload"].read().decode("utf-8"))
        self._validate_lambda_response(response=resp_payload)

    async def _invoke_lambda_async(self, request_payload: str) -> None:
        """Wrapper to make it async"""
        self._invoke_lambda_sync(request_payload=request_payload)

    def invoke(self, execution_steps: list[Step], prefix: str) -> None:
        if self.invokation_type == "sync":
            self.invoke_sync(execution_steps=execution_steps, prefix=prefix)
        self.invoke_async(execution_steps=execution_steps, prefix=prefix)

    def _verify_completion_of_invokations(self):
        # TODO: If running in Event mode, a check to see if all lambda functions have finished must be taken
        raise NotImplementedError()

    def _validate_lambda_response(self, response: dict) -> None:
        try:
            if response["statusCode"] not in [200, 202]:
                raise ValueError(
                    f"{response.get('statusCode')}: {response.get('errorMessage')}"
                )
        except KeyError as _:
            raise MisConfigurationError(response)

    def _validate_configuration_reponse(self, response: dict) -> None:
        if response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
            raise MisConfigurationError(response)

    def _update_configurations(self, configs: dict) -> None:
        response = self._client.update_function_configuration(**configs)

        self._validate_configuration_reponse(response=response)
