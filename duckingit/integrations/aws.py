import json
import typing as t
from dataclasses import dataclass

import boto3

from duckingit._exceptions import ConfigurationError
from duckingit._planner import Task


@dataclass
class SQSMessage:
    request_id: str
    message_id: str
    receipt_handle: str

    def create_entry_payload(self) -> dict[str, str]:
        return {"Id": self.message_id, "ReceiptHandle": self.receipt_handle}


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

    def invoke(self, execution_tasks: t.Set[Task], prefix: str) -> dict[str, Task]:
        request_ids = {}
        for step in execution_tasks:
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

        return self._collect_field_from_response(response=resp, field="RequestId")

    def _collect_field_from_response(self, response: dict[str, dict], field: str):
        unwrap = response.get("ResponseMetadata", None)
        if unwrap is None:
            raise ValueError(
                "Couldn't unwrap the response - `ResponseMetadata` isn't in the message"
            )

        unwrap = unwrap.get(field, None)
        if unwrap is None:
            raise ValueError(
                f"Couldn't unwrap the response - `{field}` isn't in the message"
            )

        return unwrap

    def _validate_response(self, response: dict) -> None:
        try:
            if self._collect_field_from_response(
                response=response, field="HTTPStatusCode"
            ) not in [
                200,
                202,
            ]:
                raise ValueError(
                    f"{response.get('statusCode')}: {response.get('errorMessage')}"
                )
        except KeyError:
            raise ConfigurationError(response)

    def update_lambda_configurations(self, configs: dict) -> None:
        response = self.lambda_client.update_function_configuration(**configs)

        self._validate_response(response=response)

    def update_sqs_configurations(self, name: str, configs: dict) -> None:
        response = self.sqs_client.set_queue_attributes(
            QueueUrl=name, Attributes=configs
        )
        self._validate_response(response=response)

    def _collect_items_from_sqs_message(self, message: dict) -> SQSMessage:
        request_id = (
            json.loads(message.get("Body")).get("requestContext").get("requestId")
        )
        return SQSMessage(
            request_id=request_id,
            message_id=message.get("MessageId"),
            receipt_handle=message.get("ReceiptHandle"),
        )

    def delete_messages_from_queue(
        self, name: str, entries: list[dict[str, str]]
    ) -> None:
        delete_request = {
            "QueueUrl": name,
            "Entries": entries,
        }
        resp = self.sqs_client.delete_message_batch(**delete_request)
        self._validate_response(resp)

    def poll_messages_from_queue(
        self, name: str, wait_time_seconds: int
    ) -> list[SQSMessage]:
        from duckingit._config import DuckConfig

        configs = DuckConfig()

        receive_request = {
            "QueueUrl": name,
            "MaxNumberOfMessages": configs.aws_sqs.MaxNumberOfMessages,
            "VisibilityTimeout": configs.aws_sqs.VisibilityTimeout,
            "WaitTimeSeconds": min(configs.aws_sqs.WaitTimeSeconds, wait_time_seconds),
        }
        response = self.sqs_client.receive_message(**receive_request)

        sqs_messages = list()
        if "Messages" in response:
            messages = response["Messages"]
            for message in messages:
                sqs_messages.append(self._collect_items_from_sqs_message(message))

        return sqs_messages

    def purge_queue(self, name: str) -> None:
        self.sqs_client.purge_queue(QueueUrl=name)
