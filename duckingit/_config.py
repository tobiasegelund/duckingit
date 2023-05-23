import copy
import typing as t
from dataclasses import dataclass

from duckingit._exceptions import ConfigurationError, WrongInvokationType
from duckingit._utils import cast_mapping_to_string_with_newlines
from duckingit.providers import Providers

CACHE_PREFIX = ".cache/duckingit"


class BaseConfig:
    def update(self) -> None:
        """Update the state of the ConfigSingleton"""


@dataclass
class LambdaConfig(BaseConfig):
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
            if not isinstance(value, int):
                raise ValueError(f"`Timeout` must be between an integer")

        elif name == "MemorySize":
            if not isinstance(value, int):
                raise ValueError(f"`MemorySize` must be an integer")

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
        provider = Providers.get_or_raise("aws").lambda_

        provider.update_lambda_configurations(config_dict)
        if warm_up:
            provider.lambda_.warm_up_lambda_function()


@dataclass
class SQSConfig(BaseConfig):
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
            if not isinstance(value, int):
                raise ValueError("`MaxNumberOfMessages` must be an integer")

        elif name == "VisibilityTimeout":
            if not isinstance(value, int):
                raise ValueError("`VisibilityTimeout` must be an integer")

        elif name == "WaitTimeSeconds":
            if not isinstance(value, int):
                raise ValueError("`WaitTimeSeconds` must be an integer")

        elif name == "DelaySeconds":
            if not isinstance(value, int):
                raise ValueError("`DelaySeconds` must be an integer")

        elif name == "MaximumMessageSize":
            if not isinstance(value, int):
                raise ValueError(f"`MaximumMessageSize` must be an integer")

        elif name == "MessageRetentionPeriod":
            if not isinstance(value, int):
                raise ValueError(f"`MessageRetentionPeriod` must be an integer")

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
            Providers.get_or_raise("aws").sqs.update_sqs_configurations(
                name=name, configs=config_dict
            )


@dataclass
class AWSConfig(BaseConfig):
    aws_region: str = ""
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""

    def __repr__(self) -> str:
        repr = cast_mapping_to_string_with_newlines(
            service_name="aws_config", mapping=self.__dict__
        )
        return repr

    def __setattr__(self, name: str, value: t.Any) -> None:
        if name == "aws_region":
            if not isinstance(value, str):
                raise ValueError("`aws_region` must be a string")

        elif name == "aws_access_key_id":
            if not isinstance(value, str):
                raise ValueError("`aws_access_key_id` must be a string")

        elif name == "aws_secret_access_key":
            if not isinstance(value, str):
                raise ValueError("`aws_secret_access_key` must be a string")

        else:
            raise AttributeError()

        super(AWSConfig, self).__setattr__(name, value)


@dataclass
class SessionConfig(BaseConfig):
    cache_expiration_time: int = 15
    max_invokations: int | str = "auto"
    provider: str = "aws"
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
            if not isinstance(value, str):
                raise ValueError("`provider` must be a string")

            value = Providers(value.lower()).value

        elif name == "verbose":
            if not (isinstance(value, bool)):
                raise ValueError("`verbose` must be boolean")

        else:
            raise AttributeError()

        super(SessionConfig, self).__setattr__(name, value)


@dataclass
class DuckDBConfig(BaseConfig):
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

    aws_lambda = LambdaConfig()
    aws_sqs = SQSConfig()
    aws_config = AWSConfig()
    session = SessionConfig()
    duckdb = DuckDBConfig()

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
    def show_configurations(cls):
        repr = "\n".join(
            [
                str(cls.aws_lambda),
                str(cls.aws_sqs),
                str(cls.aws_config),
                str(cls.session),
                str(cls.duckdb),
            ]
        )
        print(repr)

    services_to_be_updated: dict[str, t.Any] = {}

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
