import typing as t
import copy
from dataclasses import dataclass

from duckingit.integrations import Providers
from duckingit._exceptions import ConfigurationError


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
                raise ValueError(
                    f"`MemorySize` must be between {lower_limit} and {upper_limit} MB"
                )

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
    DelaySeconds: int = 900
    MaximumMessageSize: int = 2056
    MessageRetentionPeriod: int = 900

    def __setattr__(self, name: str, value: t.Any) -> None:
        if name == "MaxNumberOfMessages":
            if not ((value <= 10) and isinstance(value, int)):
                raise ValueError(
                    "`MaxNumberOfMessages` must be between 1 and 10 seconds"
                )

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
            k: v
            for k, v in self.__dict__.items()
            if k in ("DelaySeconds", "MaximumMessageSize", "MessageRetentionPeriod")
        }
        for name in [self.__dict__["QueueSuccess"], self.__dict__["QueueFailure"]]:
            Providers.AWS.klass.update_sqs_configurations(
                name=name, configs=config_dict
            )


# @dataclass
# class AWSConfig(ServiceConfig):
#     s3_region: str
#     s3_access_key_id: str
#     s3_secret_access_key: str


@dataclass
class SessionConfig(ServiceConfig):
    cache_expiration_time: int = 15
    max_invokations: int = 15
    provider: Providers = Providers.AWS

    def __setattr__(self, name: str, value: t.Any) -> None:
        if name == "cache_expiration_time":
            if not isinstance(value, int):
                raise ValueError("`cache expiration time` must be an integer")

        elif name == "max_invokations":
            if not isinstance(value, int):
                raise ValueError("`max invokations` must be an integer")

        elif name == "provider":
            if not (isinstance(value, str) or isinstance(value, Providers)):
                raise ValueError("`provider` must be a string")

            if isinstance(value, str):
                value = Providers(value.lower())

        else:
            raise AttributeError()

        super(SessionConfig, self).__setattr__(name, value)


@dataclass
class DuckDBConfig(ServiceConfig):
    database: str = ":memory:"
    read_only: bool = False

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


class ConfigSingleton(object):
    aws_lambda_config = LambdaConfig()
    aws_sqs_config = SQSConfig()
    session_config = SessionConfig()
    duckdb_config = DuckDBConfig()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(ConfigSingleton, cls).__new__(cls)
        return cls.instance

    def __repr__(self) -> str:
        return f"{self.__dict__}"

    def __getattr__(self, name: str):
        keys = name.split(".")
        attr = self

        # Traverse config tree
        try:
            for key in keys:
                attr = getattr(attr, key)

        except AttributeError as _:
            raise ConfigurationError(f"Configuration `{name}` doesn't exists")

        return attr

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


class DuckConfig:
    config_singleton = ConfigSingleton()
    services_to_be_updated = {}

    def __repr__(self) -> str:
        return "DuckConfig\n_________\n"

    def set(self, name: str, value: t.Any):
        keys = name.split(".")

        service = self.config_singleton
        attr = keys.pop()

        # Traverse config tree
        try:
            for key in keys:
                service = getattr(service, key)

            setattr(service, attr, value)
        except AttributeError as _:
            raise ConfigurationError(f"Configuration `{name}` doesn't exists")

        key = ".".join(keys)  # Concat list of keys without the attr
        self.services_to_be_updated[key] = service

        return self

    def update(self):
        for _, service in self.services_to_be_updated.items():
            service.update()
