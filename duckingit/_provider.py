import json
import asyncio
from typing import Literal

import boto3

from ._parser import QueryParser


class Provider:
    def invoke(self, queries: list[str], prefix: str) -> None:
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

    def invoke_sync(self, queries: list[str], prefix: str) -> None:
        for query in queries:
            # TODO: Allow other naming options than just hashed
            query_hashed = QueryParser.hash_query(query)
            key = prefix + "/" + query_hashed + ".parquet"
            request_payload = json.dumps({"query": query, "key": key})
            _ = self._invoke_lambda_sync(request_payload=request_payload)

    def invoke_async(self, queries: list[str], prefix: str):
        asyncio.run(self._invoke_async(queries=queries, prefix=prefix))

    async def _invoke_async(self, queries: list[str], prefix: str) -> None:
        tasks = []
        for query in queries:
            query_hashed = QueryParser.hash_query(query)
            key = prefix + "/" + query_hashed + ".parquet"
            request_payload = json.dumps({"query": query, "key": key})

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
        self._raise_error_if_no_success(response=resp_payload)

    async def _invoke_lambda_async(self, request_payload: str) -> None:
        """Wrapper to make it async"""
        self._invoke_lambda_sync(request_payload=request_payload)

    def invoke(self, queries: list[str], prefix: str) -> None:
        if self.invokation_type == "sync":
            self.invoke_sync(queries=queries, prefix=prefix)
        self.invoke_async(queries=queries, prefix=prefix)

    def _verify_invokations_have_completed(self):
        """TODO: If running in Event mode, a check to see if all lambda functions have finished must be taken"""
        pass

    def _raise_error_if_no_success(self, response: dict) -> None:
        if response["statusCode"] not in [200, 202]:
            raise ValueError(
                f"{response.get('statusCode')}: {response.get('errorMessage')}"
            )


# from enum import Enum

# class GCP:
#     pass


# class Azure:
#     pass


# class ProviderType(Enum):
#     AWS = AWS
#     GCP = GCP
#     Azure = Azure
