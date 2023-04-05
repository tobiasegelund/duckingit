import pytest

from duckingit._config import load_aws_credentials


def test_load_aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "1234")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "5678")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-central")

    credentials = load_aws_credentials()

    assert credentials.to_dict() == {
        "aws_access_key": "1234",
        "aws_secret_access_key": "5678",
        "aws_default_region": "eu-central",
    }
