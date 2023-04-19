import json
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

    def invoke(self, execution_steps: list[Step], prefix: str) -> None:
        for step in execution_steps:
            key = f"{prefix}/{step.subquery_hashed}.parquet"
            request_payload = json.dumps({"query": step.subquery, "key": key})
            _ = self._invoke_lambda(request_payload=request_payload)

    def _invoke_lambda(self, request_payload: str) -> None:
        resp = self._client.invoke(
            FunctionName=self.function_name,
            Payload=request_payload,
            InvocationType="RequestResponse",  # Event
        )

        resp_payload = json.loads(resp["Payload"].read().decode("utf-8"))
        self._validate_lambda_response(response=resp_payload)

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
