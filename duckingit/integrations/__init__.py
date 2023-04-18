from enum import Enum

from .aws import AWS


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
