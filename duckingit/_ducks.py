class RaftOfDucks:
    def __init__(self, lambda_function: str, **kwargs) -> None:
        self.lambda_function = lambda_function
        self.kwargs = kwargs

    def swim(self, sql: str, invokations: int = 1):
        """Divide the

        Args:
            function_name, Optional(str):
                Defaults to create a new Lambda function
            invokations, int:
                Defaults to 1
        """
        return
