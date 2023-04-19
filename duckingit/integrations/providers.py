import os
from enum import Enum

from duckingit.integrations.aws import AWS


class Providers(Enum):
    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"

    @property
    def klass(self):
        providers = {self.AWS: AWS}

        try:
            # _klass = providers[self]
            return providers[self]

        except KeyError:
            raise NotImplementedError("Currently, it's only implemented for AWS Lambda")

    @property
    def fs(self) -> str:
        _fs = {self.AWS: "s3", self.GCP: "gcp"}
        return _fs[self]

    @property
    def credentials(self) -> str:
        s3 = f"""
            SET s3_region='{os.getenv("AWS_DEFAULT_REGION", None)}';
            SET s3_access_key_id='{os.getenv("AWS_ACCESS_KEY_ID", None)}';
            SET s3_secret_access_key='{os.getenv("AWS_SECRET_ACCESS_KEY", None)}';
        """

        gcp = f"""
            SET s3_endpoint='{os.getenv("S3_ENDPOINT", None)}';
            SET s3_access_key_id='{os.getenv("AWS_ACCESS_KEY_ID", None)}';
            SET s3_secret_access_key='{os.getenv("AWS_SECRET_ACCESS_KEY", None)}';
        """

        _credentials = {self.AWS: s3, self.GCP: gcp}

        return _credentials[self]
