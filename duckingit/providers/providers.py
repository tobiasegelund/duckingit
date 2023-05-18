import os
import typing as t
from enum import Enum

from duckingit.providers.aws import AWS


class Providers(Enum):
    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"

    @property
    def klass(self):
        providers = {self.AWS: AWS}

        try:
            return providers[self]()  # type: ignore

        except KeyError:
            raise NotImplementedError("Currently, it's only implemented for AWS Lambda")

    @classmethod
    def get_or_raise(cls, name: str):
        try:
            return cls(name.lower())
        except ValueError as e:
            raise ValueError(f"Unknown provider `{name}`") from e

    @property
    def fs(self) -> str:
        _fs = {self.AWS: "s3", self.GCP: "gcp"}
        return _fs[self]  # type: ignore

    @property
    def credentials(self) -> str:
        from duckingit import DuckConfig

        aws_settings: dict[str, t.Optional[str]] = {}
        for key in ["aws_region", "aws_access_key_id", "aws_secret_access_key"]:
            val = getattr(DuckConfig().aws_config, key)
            if val == "":
                val = os.getenv(key.upper(), None)
            aws_settings[key] = val

        s3 = """
            SET s3_region='{aws_region}';
            SET s3_access_key_id='{aws_access_key_id}';
            SET s3_secret_access_key='{aws_secret_access_key}';
        """.format(
            **aws_settings
        )

        gcp = f"""
            SET s3_endpoint='{os.getenv("S3_ENDPOINT", None)}';
            SET s3_access_key_id='{os.getenv("AWS_ACCESS_KEY_ID", None)}';
            SET s3_secret_access_key='{os.getenv("AWS_SECRET_ACCESS_KEY", None)}';
        """

        _credentials = {self.AWS: s3, self.GCP: gcp}

        return _credentials[self]  # type: ignore
