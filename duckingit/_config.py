from duckingit.integrations import Providers


class LambdaConfig:
    def __init__(self, function_name: str = "DuckExecutor") -> None:
        self.function_name = function_name

        self._configs: dict[str, str | int] = {"FunctionName": function_name}
        self._provider = Providers.AWS.klass(function_name=self.function_name)

    def __repr__(self) -> str:
        return f"{self._configs}"

    def _change_memory_size(self, memory_size: int) -> None:
        if memory_size < 128 or memory_size > 10_240:
            raise ValueError("Memory size must be between 128 or 10,240 MB")
        self._configs["MemorySize"] = memory_size

    def _change_timeout(self, timeout: int) -> None:
        if timeout < 3 or timeout > 900:
            raise ValueError("Timeout must be between 3 or 900 seconds")
        self._configs["Timeout"] = timeout

    def _warm_up(self) -> None:
        self._configs["WARMUP"] = True

    def update(self) -> None:
        warm_up = False
        if "WARMUP" in self._configs:
            warm_up = self._configs.pop("WARMUP")

        self._provider._update_configurations(configs=self._configs)

        if warm_up:
            self._provider.warm_up()


class DuckConfig:
    """Class to store configurations of the session

    DuckConfig can be used to update serverless function's configurations, as well as
    a max number of invokations and cache expiration time.

    Usage:
        >>> DuckConfig().memory_size(128).timeout(30).warm_up().update()
        >>> DuckConfig().max_invokations(100).update()
    """

    def __init__(self, function_name: str = "DuckExecutor") -> None:
        self._function_name = function_name

        self._max_invokations: int | None = None
        self._cache_expiration_time = 15  # 10 minutes default
        self._lambda_config = LambdaConfig(function_name=function_name)

    def __repr__(self) -> str:
        return f"Configurations<CACHE_EXPIRATION_TIME={self._cache_expiration_time} \
| MAX_INVOKATIONS={self._max_invokations} | LAMBDA_CONFIG={self._lambda_config}>"

    def memory_size(self, memory_size: int):
        if not isinstance(memory_size, int):
            raise ValueError(
                f"Memory size must be an integer - {type(memory_size)} was provided"
            )
        self._lambda_config._change_memory_size(memory_size=memory_size)
        return self

    def timeout(self, timeout: int):
        if not isinstance(timeout, int):
            raise ValueError(
                f"Timeout must be an integer - {type(timeout)} was provided"
            )
        self._lambda_config._change_timeout(timeout=timeout)
        return self

    def max_invokations(self, invokations: int):
        # TODO: Move settings to LambdaConfig in order to force .update() to update configs
        if not isinstance(invokations, int):
            raise ValueError(
                f"Invokations must be an integer - {type(invokations)} was provided"
            )
        self._max_invokations = invokations
        return self

    def cache_expiration_time(self, time: int = 15):
        """Expiration time of cached objects in minutes"""
        # TODO: Move settings to LambdaConfig in order to force .update() to update configs
        if not isinstance(time, int):
            raise ValueError(f"Time must be an integer - {type(time)} was provided")
        self._cache_expiration_time = time
        return self

    def warm_up(self):
        self._lambda_config._warm_up()
        return self

    def update(self):
        self._lambda_config.update()
