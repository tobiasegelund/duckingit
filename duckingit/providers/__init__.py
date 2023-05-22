from enum import Enum

from duckingit.providers.aws import AWS


class Providers(Enum):
    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"

    @classmethod
    def get_or_raise(cls, name: str):
        providers = {cls.AWS: AWS}

        try:
            return providers[cls(name.lower())]()
        except ValueError as e:
            raise ValueError(f"Unknown provider `{name}`") from e
