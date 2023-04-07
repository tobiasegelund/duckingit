import json
import asyncio
from typing import Literal

import boto3


class Provider:
    def invoke(self, queries: list[str]) -> list[dict]:
        raise NotImplementedError()


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

    def invoke_sync(self, queries: list[str]) -> list[dict]:
        output = list()
        for query in queries:
            request_payload = json.dumps({"body": query})
            result = self._invoke_lambda_sync(request_payload=request_payload)
            output.append(result)

        return output

    def invoke_async(self, queries: list[str]) -> list[dict]:
        return asyncio.run(self._invoke_async(queries=queries))

    async def _invoke_async(self, queries: list[str]):
        tasks = []
        for query in queries:
            request_payload = json.dumps({"body": query})

            task = asyncio.create_task(self._invoke_lambda_async(request_payload))
            tasks.append(task)
        tasks_to_run = await asyncio.gather(*tasks)
        return tasks_to_run

    def _invoke_lambda_sync(self, request_payload: str) -> dict:
        resp = self._client.invoke(
            FunctionName=self.function_name,
            Payload=request_payload,
            InvocationType="RequestResponse",
        )

        resp_payload = json.loads(resp["Payload"].read().decode("utf-8"))
        self._handle_error(resp=resp_payload)

        return resp_payload

    async def _invoke_lambda_async(self, request_payload: str):
        """Wrapper to make it async"""
        return self._invoke_lambda_sync(request_payload=request_payload)

    def invoke(self, queries: list[str]) -> list[dict]:
        if self.invokation_type == "sync":
            return self.invoke_sync(queries=queries)
        return self.invoke_async(queries=queries)

    def _handle_error(self, resp: dict) -> None:
        if "errorMessage" in resp.keys():
            # TODO: Create user-defined exception
            raise ValueError(f"{resp.get('errorType')}: {resp.get('errorMessage')}")


class MockAWS(AWS):
    def _invoke_lambda_sync(self, request_payload: str) -> dict:
        return {
            "data": [(100, "John", "Doe"), (101, "Eric", "Doe"), (102, "Maria", "Doe")],
            "dtypes": ["BIGINT", "VARCHAR", "VARCHAR"],
            "columns": ["id", "first_name", "last_name"],
        }


# from enum import Enum

# class GCP:
#     pass


# class Azure:
#     pass


# class ProviderType(Enum):
#     AWS = AWS
#     GCP = GCP
#     Azure = Azure
