# TODO: Add check of default credentials in ~/.aws/ folder
import os
from dataclasses import dataclass, asdict

import duckdb


@dataclass(frozen=True)
class AWSCredentials:
    aws_access_key: str
    aws_secret_access_key: str
    aws_default_region: str

    def to_dict(self):
        return {k: str(v) for k, v in asdict(self).items()}


def load_aws_credentials() -> AWSCredentials:
    aws_access_key_name = "AWS_ACCESS_KEY_ID"
    aws_secret_key_name = "AWS_SECRET_ACCESS_KEY"
    aws_default_region_name = "AWS_DEFAULT_REGION"

    access_key = os.environ.get(aws_access_key_name, False)
    secret_key = os.environ.get(aws_secret_key_name, False)
    default_region = os.environ.get(aws_default_region_name, False)

    if not all([access_key, secret_key, default_region]):
        raise EnvironmentError(
            f"Couldn't find any AWS credentials as environmental variables. Please \
update {aws_access_key_name}, {aws_secret_key_name} and {aws_default_region_name}"
        )

    return AWSCredentials(
        aws_access_key=access_key,
        aws_secret_access_key=secret_key,
        aws_default_region=default_region,
    )


def install_httpfs():
    db = duckdb.connect()
    db.execute("INSTALL httpfs;")
