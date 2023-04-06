import json

import boto3


class Provider:
    def invoke(self, queries: list[str]) -> list[dict]:
        raise NotImplementedError()


class AWS(Provider):
    def __init__(self, function_name: str) -> None:
        self.function_name = function_name

        self._client = boto3.client("lambda")

    def invoke(self, queries: list[str]) -> list[dict]:
        # TODO: Rewrite to async
        output = list()
        for query in queries:
            payload = query

            resp = self._client.invoke(
                FunctionName=self.function_name,
                Payload=payload,
                InvocationType="RequestResponse",
            )
            result = json.loads(resp["Payload"].read().decode("utf-8"))
            output.append(result)

        return output


# from enum import Enum

# class GCP:
#     pass


# class Azure:
#     pass


# class ProviderType(Enum):
#     AWS = AWS
#     GCP = GCP
#     Azure = Azure
