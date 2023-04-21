import json

import boto3

from duckingit._exceptions import ConfigurationError
from duckingit._planner import Step


class AWS:
    lambda_client = boto3.client("lambda")
    sqs_client = boto3.client("sqs")

    def warm_up_lambda_function(self) -> None:
        """Method to avoid cold starts"""
        from duckingit._config import DuckConfig

        _ = self.lambda_client.invoke(
            FunctionName=DuckConfig().aws_lambda.FunctionName,
            Payload=json.dumps({"WARMUP": 1}),
            InvocationType="RequestResponse",
        )

    def invoke(self, execution_steps: list[Step], prefix: str) -> dict[str, Step]:
        request_ids = {}
        for step in execution_steps:
            key = f"{prefix}/{step.subquery_hashed}.parquet"
            request_payload = json.dumps({"query": step.subquery, "key": key})
            request_id = self._invoke_lambda(request_payload=request_payload)

            request_ids[request_id] = step
        return request_ids

    def _invoke_lambda(self, request_payload: str):
        from duckingit._config import DuckConfig

        resp = self.lambda_client.invoke(
            FunctionName=DuckConfig().aws_lambda.FunctionName,
            Payload=request_payload,
            InvocationType="Event",  # RequestResponse
        )
        self._validate_response(response=resp)

        return self._unwrap_response(response=resp, field="RequestId")

    def _unwrap_response(self, response: dict[str, dict], field: str):
        unwrap = response.get("ResponseMetadata", None)
        if unwrap is None:
            raise ValueError(
                "Couldn't unwrap the response - `ResponseMetadata` isn't in the message"
            )

        unwrap = unwrap.get(field, None)
        if unwrap is None:
            raise ValueError(f"Couldn't unwrap the response - `{field}` isn't in the message")

        return unwrap

    def _validate_response(self, response: dict) -> None:
        try:
            if self._unwrap_response(response=response, field="HTTPStatusCode") not in [
                200,
                202,
            ]:
                raise ValueError(f"{response.get('statusCode')}: {response.get('errorMessage')}")
        except KeyError:
            raise ConfigurationError(response)

    def update_lambda_configurations(self, configs: dict) -> None:
        response = self.lambda_client.update_function_configuration(**configs)

        self._validate_response(response=response)

    def update_sqs_configurations(self, name: str, configs: dict) -> None:
        response = self.sqs_client.set_queue_attributes(QueueUrl=name, Attributes=configs)
        self._validate_response(response=response)

    def _unwrap_request_id_from_queue_message(self, message: dict) -> str:
        request_id = json.loads(message.get("Body")).get("requestContext").get("requestId")
        return request_id

    def _delete_messages_from_sqs_queue(self, name: str, entries: list(dict[str, str])) -> None:
        delete_request = {
            "QueueUrl": name,
            "Entries": entries,
        }
        self.sqs_client.Entries(**delete_request)

    def poll_messages_from_queue(self, name: str, delete_messages: bool) -> list[str]:
        from duckingit._config import DuckConfig

        configs = DuckConfig()

        receive_request = {
            "QueueUrl": name,
            "MaxNumberOfMessages": configs.aws_sqs.MaxNumberOfMessages,
            "VisibilityTimeout": configs.aws_sqs.VisibilityTimeout,
            "WaitTimeSeconds": configs.aws_sqs.WaitTimeSeconds,
        }
        response = self.sqs_client.receive_message(**receive_request)

        request_ids = []
        entries = list()
        if "Messages" in response:
            messages = response["Messages"]
            for message in messages:
                request_ids.append(self._unwrap_request_id_from_queue_message(message))
                entries.append({message.get("MessageId"): message.get("ReceiptHandle")})

        if delete_messages:
            self._delete_messages_from_sqs_queue(name=name, entries=entries)

        return request_ids

    def poll_messages_from_success_queue(self, delete_messages: bool = True) -> list[str]:
        from duckingit._config import DuckConfig

        return self.poll_messages_from_queue(
            name=DuckConfig().aws_sqs.QueueSuccess, delete_messages=delete_messages
        )

    def poll_messages_from_failure_queue(self, delete_messages: bool = False) -> list[str]:
        from duckingit._config import DuckConfig

        return self.poll_messages_from_queue(
            name=DuckConfig().aws_sqs.QueueFailure, delete_messages=delete_messages
        )
