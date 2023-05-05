"""
TODO: Perhaps remove hardcoded limits and let boto3 raise the exception
Trade-off: Fast vs slow response
"""
import copy
import typing as t
from dataclasses import dataclass

from duckingit._exceptions import ConfigurationError, WrongInvokationType
from duckingit._utils import cast_mapping_to_string_with_newlines
from duckingit.integrations import Providers

CACHE_PREFIX = ".cache/duckingit"


class ServiceConfig:
    def update(self) -> None:
        """Update the state of the ConfigSingleton"""
        pass


@dataclass
class LambdaConfig(ServiceConfig):
    FunctionName: str = "DuckExecutor"
    MemorySize: int = 128
    Timeout: int = 30
    WarmUp: bool = False

    def __repr__(self) -> str:
        repr = cast_mapping_to_string_with_newlines(
            service_name="aws_lambda", mapping=self.__dict__
        )
        return repr

    def __setattr__(self, name: str, value: t.Any) -> None:
        if name == "Timeout":
            lower_limit = 3
            upper_limit = 900
            if not ((lower_limit <= value <= upper_limit) and isinstance(value, int)):
                raise ValueError(
                    f"`Timeout` must be between {lower_limit} and {upper_limit} seconds"
                )

        elif name == "MemorySize":
            lower_limit = 128
            upper_limit = 10240
            if not ((lower_limit <= value <= upper_limit) and isinstance(value, int)):
                raise ValueError(f"`MemorySize` must be between {lower_limit} and {upper_limit} MB")

        elif name == "WarmUp":
            if not isinstance(value, bool):
                raise ValueError("`WarmUp` must be a boolean")

        elif name == "FunctionName":
            if not isinstance(value, str):
                raise ValueError("`FunctionName` must be a string")

        else:
            raise AttributeError()

        super(LambdaConfig, self).__setattr__(name, value)

    def update(self):
        config_dict = copy.deepcopy(self.__dict__)
        warm_up = config_dict.pop("WarmUp")

        Providers.AWS.klass.update_lambda_configurations(config_dict)

        if warm_up:
            Providers.AWS.klass.warm_up_lambda_function()


@dataclass
class SQSConfig(ServiceConfig):
    QueueSuccess: str = "DuckSuccess"
    QueueFailure: str = "DuckFailure"

    # When receiving messages
    MaxNumberOfMessages: int = 10
    VisibilityTimeout: int = 5
    WaitTimeSeconds: int = 5

    # Configs on Queue itself
    DelaySeconds: int = 0
    MaximumMessageSize: int = 2056
    MessageRetentionPeriod: int = 900

    def __repr__(self) -> str:
        repr = cast_mapping_to_string_with_newlines(service_name="aws_sqs", mapping=self.__dict__)
        return repr

    def __setattr__(self, name: str, value: t.Any) -> None:
        if name == "MaxNumberOfMessages":
            if not ((value <= 10) and isinstance(value, int)):
                raise ValueError("`MaxNumberOfMessages` must be between 1 and 10 seconds")

        elif name == "VisibilityTimeout":
            if not ((value <= 60) and isinstance(value, int)):
                raise ValueError("`VisibilityTimeout` must be between 0 and 60 seconds")

        elif name == "WaitTimeSeconds":
            if not ((value <= 60) and isinstance(value, int)):
                raise ValueError("`WaitTimeSeconds` must be between 0 and 60 seconds")

        elif name == "DelaySeconds":
            if not ((value <= 900) and isinstance(value, int)):
                raise ValueError("`DelaySeconds` must be between 0 and 900 seconds")

        elif name == "MaximumMessageSize":
            lower_limit = 1024
            upper_limit = 262_144
            if not ((lower_limit <= value <= upper_limit) and isinstance(value, int)):
                raise ValueError(
                    f"`MaximumMessageSize` must be between {lower_limit} and {upper_limit} KiB"
                )

        elif name == "MessageRetentionPeriod":
            lower_limit = 60  # 1 minute
            upper_limit = 1_209_600  # 14 days
            if not ((lower_limit <= value <= upper_limit) and isinstance(value, int)):
                raise ValueError(
                    f"`MessageRetentionPeriod` must be between {lower_limit} and {upper_limit} seconds"
                )

        elif name == "QueueSuccess":
            if not isinstance(value, str):
                raise ValueError("`QueueSuccess` must be a string")

        elif name == "QueueFailure":
            if not isinstance(value, str):
                raise ValueError("`QueueFailure` must be a string")

        else:
            raise AttributeError()

        super(SQSConfig, self).__setattr__(name, value)

    def update(self) -> None:
        config_dict = {
            k: str(v)
            for k, v in self.__dict__.items()
            if k in ("DelaySeconds", "MaximumMessageSize", "MessageRetentionPeriod")
        }
        for name in [self.__dict__["QueueSuccess"], self.__dict__["QueueFailure"]]:
            Providers.AWS.klass.update_sqs_configurations(name=name, configs=config_dict)


# @dataclass
# class AWSConfig(ServiceConfig):
#     s3_region: str
#     s3_access_key_id: str
#     s3_secret_access_key: str


@dataclass
class SessionConfig(ServiceConfig):
    cache_expiration_time: int = 15
    max_invokations: int | str = "auto"
    provider: Providers = Providers.AWS
    verbose: bool = False

    def __repr__(self) -> str:
        repr = cast_mapping_to_string_with_newlines(service_name="session", mapping=self.__dict__)
        return repr

    def __setattr__(self, name: str, value: t.Any) -> None:
        if name == "cache_expiration_time":
            if not isinstance(value, int):
                raise ValueError("`cache expiration time` must be an integer")

        elif name == "max_invokations":
            if not (isinstance(value, int) or isinstance(value, str)):
                if isinstance(value, str):
                    if value != "auto":
                        raise WrongInvokationType("`value` can only be 'auto' or an integer")
                raise ValueError("`max invokations` must be an integer")

        elif name == "provider":
            if not (isinstance(value, str) or isinstance(value, Providers)):
                raise ValueError("`provider` must be a string")

            if isinstance(value, str):
                value = Providers(value.lower())

        elif name == "verbose":
            if not (isinstance(value, bool)):
                raise ValueError("`verbose` must be boolean")

        else:
            raise AttributeError()

        super(SessionConfig, self).__setattr__(name, value)


@dataclass
class DuckDBConfig(ServiceConfig):
    database: str = ":memory:"
    read_only: bool = False

    def __repr__(self) -> str:
        repr = cast_mapping_to_string_with_newlines(service_name="duckdb", mapping=self.__dict__)
        return repr

    def __setattr__(self, name: str, value: t.Any) -> None:
        if name == "database":
            if not isinstance(value, str):
                raise ValueError("`database` must be a string")

        elif name == "read_only":
            if not isinstance(value, bool):
                raise ValueError("`read only` must be a boolean")

        else:
            raise AttributeError()

        super(DuckDBConfig, self).__setattr__(name, value)


class DuckConfig:
    """A class that to store configurations

    Note the class follows the singleton design pattern
    """

    aws_lambda_config = LambdaConfig()
    aws_sqs_config = SQSConfig()
    session_config = SessionConfig()
    duckdb_config = DuckDBConfig()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(DuckConfig, cls).__new__(cls)
        return cls.instance

    def __getattr__(self, name: str):
        keys = name.split(".")
        attr = self

        # Traverse config tree
        try:
            for key in keys:
                attr = getattr(attr, key)

        except AttributeError:
            raise ConfigurationError(f"Configuration `{name}` doesn't exists")

        return attr

    @classmethod
    @property
    def show_configurations(cls):
        repr = "\n".join(
            [
                str(cls.aws_lambda_config),
                str(cls.aws_sqs_config),
                str(cls.session_config),
                str(cls.duckdb_config),
            ]
        )
        print(repr)

    @property
    def aws_lambda(self):
        return self.aws_lambda_config

    @property
    def aws_sqs(self):
        return self.aws_sqs_config

    @property
    def session(self):
        return self.session_config

    @property
    def duckdb(self):
        return self.duckdb_config

    services_to_be_updated = {}

    def set(self, name: str, value: t.Any):
        keys = name.split(".")

        service = self
        attr = keys.pop()

        # Traverse config tree
        try:
            for key in keys:
                service = getattr(service, key)

            setattr(service, attr, value)
        except AttributeError:
            raise ConfigurationError(f"Configuration `{name}` doesn't exists")

        key = ".".join(keys)  # Concat list of keys without the attr
        self.services_to_be_updated[key] = service

        return self

    def update(self):
        for _, service in self.services_to_be_updated.items():
            service.update()
