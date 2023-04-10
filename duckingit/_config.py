from ._provider import AWS


class LambdaConfig:
    def __init__(self, function_name: str = "DuckExecutor") -> None:
        self.function_name = function_name

        self._configs: dict[str, str | int] = {"FunctionName": function_name}
        self._provider = AWS(function_name=self.function_name)

    def _change_memory_size(self, memory_size: int) -> None:
        if memory_size < 128 or memory_size > 10240:
            raise ValueError("Memory size must between 128 or 10240 MB")
        self._configs["MemorySize"] = memory_size

    def _change_timeout(self, timeout: int) -> None:
        if timeout < 3 or timeout > 900:
            raise ValueError("Timeout must between 3 or 900 seconds")
        self._configs["Timeout"] = timeout

    def update(self) -> None:
        self._provider._update_configurations(configs=self._configs)


class DuckConfig:
    """Class to configure the serverless function"""

    def __init__(self, function_name: str = "DuckExecutor") -> None:
        self.function_name = function_name

        self._lambda_config = LambdaConfig(function_name=function_name)

    def memory_size(self, memory_size: int):
        self._lambda_config._change_memory_size(memory_size=memory_size)
        return self

    def timeout(self, timeout: int):
        self._lambda_config._change_timeout(timeout=timeout)
        return self

    def update(self):
        return self._lambda_config.update()
