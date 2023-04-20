import json

import boto3

from duckingit._exceptions import ConfigurationError
from duckingit._planner import Step


class AWS:
    lambda_client = boto3.client("lambda")
    sqs_client = boto3.client("sqs")

    def warm_up_lambda_function(self) -> None:
        """Method to avoid cold starts"""
        from duckingit._config import ConfigSingleton

        _ = self.lambda_client.invoke(
            FunctionName=ConfigSingleton().aws_lambda.FunctionName,
            Payload=json.dumps({"WARMUP": 1}),
            InvocationType="RequestResponse",
        )

    def invoke(self, execution_steps: list[Step], prefix: str) -> dict[str, Step]:
        invokation_ids = {}
        for step in execution_steps:
            key = f"{prefix}/{step.subquery_hashed}.parquet"
            request_payload = json.dumps({"query": step.subquery, "key": key})
            invokatin_id = self._invoke_lambda(request_payload=request_payload)

            invokation_ids[invokatin_id] = step
        return invokation_ids

    def _invoke_lambda(self, request_payload: str):
        from duckingit._config import ConfigSingleton

        resp = self.lambda_client.invoke(
            FunctionName=ConfigSingleton().aws_lambda.FunctionName,
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
            raise ValueError(
                f"Couldn't unwrap the response - `{field}` isn't in the message"
            )

        return unwrap

    def _validate_response(self, response: dict) -> None:
        try:
            if self._unwrap_response(response=response, field="HTTPStatusCode") not in [
                200,
                202,
            ]:
                raise ValueError(
                    f"{response.get('statusCode')}: {response.get('errorMessage')}"
                )
        except KeyError as _:
            raise ConfigurationError(response)

    def update_lambda_configurations(self, configs: dict) -> None:
        response = self.lambda_client.update_function_configuration(**configs)

        self._validate_response(response=response)

    def update_sqs_configurations(self, name: str, configs: dict) -> None:
        response = self.sqs_client.set_queue_attributes(
            QueueUrl=name, Attributes=configs
        )
        self._validate_response(response=response)

    def poll_messages_from_queue(self, name: str):
        from duckingit._config import ConfigSingleton

        configs = ConfigSingleton()

        receive_request = {
            "QueueUrl": name,
            "MaxNumberOfMessages": configs.aws_sqs.MaxNumberOfMessages,
            "VisibilityTimeout": configs.aws_sqs.VisibilityTimeout,
            "WaitTimeSeconds": configs.aws_sqs.WaitTimeSeconds,
        }

        # Receive messages from the queue
        response = self.sqs_client.receive_message(**receive_request)

        # Process the messages
        if "Messages" in response:
            messages = response["Messages"]
            for message in messages:
                print(message["Body"])
                # Delete the message from the queue
                delete_request = {
                    "QueueUrl": name,
                    "ReceiptHandle": message["ReceiptHandle"],
                }
                self.sqs_client.delete_message(**delete_request)
        else:
            print("No messages in the queue")

    def purge_queue(self, url: str) -> None:
        """Deletes all available messages in queue"""
        self.sqs_client.purge_queue(url)
