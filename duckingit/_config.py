from duckingit.integrations import AWS


class LambdaConfig:
    def __init__(self, function_name: str = "DuckExecutor") -> None:
        self.function_name = function_name

        self._configs: dict[str, str | int] = {"FunctionName": function_name}
        self._provider = AWS(function_name=self.function_name)

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
    """Class to configure the serverless function"""

    def __init__(self, function_name: str = "DuckExecutor") -> None:
        self._function_name = function_name

        self._lambda_config = LambdaConfig(function_name=function_name)

    def memory_size(self, memory_size: int):
        if not isinstance(memory_size, int):
            raise ValueError(
                f"Memory size must be integer - {type(memory_size)} was tried"
            )
        self._lambda_config._change_memory_size(memory_size=memory_size)
        return self

    def timeout(self, timeout: int):
        if not isinstance(timeout, int):
            raise ValueError(f"Timeout must be integer - {type(timeout)} was tried")
        self._lambda_config._change_timeout(timeout=timeout)
        return self

    def max_invokations(self, invokations: int):
        if not isinstance(invokations, int):
            raise ValueError(
                f"Invokations must be integer - {type(invokations)} was tried"
            )
        self._max_invokations = invokations
        return self

    def warm_up(self):
        self._lambda_config._warm_up()
        return self

    def update(self):
        self._lambda_config.update()
