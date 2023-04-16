import duckdb


class Dataset:
    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self.conn = conn

    def show(self):
        pass

    def print_shema(self):
        pass
