from duckingit._planner import Step


class Provider:
    def invoke(self, execution_steps: list[Step], prefix: str) -> None:
        raise NotImplementedError()


# class GCP:
#     pass


# class Azure:
#     pass
