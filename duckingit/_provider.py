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
            payload = json.dumps({"body": query})

            resp = self._client.invoke(
                FunctionName=self.function_name,
                Payload=payload,
                InvocationType="RequestResponse",
            )
            result = json.loads(resp["Payload"].read().decode("utf-8"))

            self._handle_error(resp=result)

            output.append(result)

        return output

    def _handle_error(self, resp: dict) -> None:
        if "errorMessage" in resp.keys():
            # TODO: Create user-defined exception
            raise ValueError(f"{resp.get('errorType')}: {resp.get('errorMessage')}")


class MockProvider(Provider):
    def invoke(self, queries: list[str]) -> list[dict]:
        pass


# from enum import Enum

# class GCP:
#     pass


# class Azure:
#     pass


# class ProviderType(Enum):
#     AWS = AWS
#     GCP = GCP
#     Azure = Azure
