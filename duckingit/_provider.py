import boto3

from ._config import load_aws_credentials, AWSCredentials


class AWS:
    def __init__(self, function_name: str) -> None:
        self.function_name = function_name

        self._client = boto3.client("lambda")
        self._credentials: AWSCredentials = load_aws_credentials()

    @property
    def credentials(self) -> AWSCredentials:
        return self._credentials

    def run(self, query: str):
        payload = query

        resp = self._client.invoke(
            FunctionName=self.function_name,
            Payload=payload,
            InvocationType="RequestResponse",
        )
        return resp


# from enum import Enum

# class GCP:
#     pass


# class Azure:
#     pass


# class Provider(Enum):
#     AWS = AWS
#     GCP = GCP
#     Azure = Azure
