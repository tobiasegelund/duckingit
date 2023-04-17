import typing as t
from enum import Enum

import duckdb

from duckingit._parser import Query
from duckingit._planner import Plan
from duckingit._exceptions import DatasetExistError
from duckingit.integrations import Providers

if t.TYPE_CHECKING:
    from duckingit._session import DuckSession


class Modes(Enum):
    """
    Modes:
        append: Append the data to the table
        overwrite: Overwrite the data to the table (e.g. DELETE & INSERT)
        write: Write the data to the table (if the table exists an exception is raised)
    """

    APPEND = "append"
    OVERWRITE = "overwrite"
    WRITE = "write"
    # UPDATE = "update"
    # DELETE = "delete"

    @property
    def command(self):
        def append(conn: duckdb.DuckDBPyConnection, *args, **kwargs):
            # TODO: Find missing partitions
            raise NotImplementedError()

        def overwrite(conn: duckdb.DuckDBPyConnection, *args, **kwargs):
            # TODO How to handle two scenerios
            pass

        def write(
            conn: duckdb.DuckDBPyConnection, source: str, *args, **kwargs
        ) -> None:
            resp = conn.sql(f"SELECT COUNT(*) AS cnt FROM GLOB('{source}/*')")
            if resp.fetchall()[0][0]:  # Anything other than 0 it raises an exception
                raise DatasetExistError(
                    "The dataset exists already. You may want to use 'overwrite' as mode instead?"
                )

        _funcs = {
            self.APPEND: append,
            self.OVERWRITE: overwrite,
            self.WRITE: write,
        }

        return _funcs[self]


class Formats(Enum):
    DELTA = "delta"
    ICEBERG = "iceberg"
    HUDI = "hudi"


class DatasetWriter:
    # write_to, str | None: The prefix to write to, e.g. 's3://BUCKET_NAME/data'
    #     Defaults to .cache/duckingit/ prefix
    _mode: Modes = Modes.WRITE

    def __init__(self, session: "DuckSession", dataset: "Dataset") -> None:
        self._session = session
        self._dataset = dataset

    def _create_tmp_table(self, table_name: str, source: str) -> None:
        self._session.conn.sql(
            f"""
            CREATE TEMP TABLE {table_name} AS (
                SELECT * FROM READ_PARQUET(['{source}/*'])
            )
            """
        )

    def mode(self, value: str = "write"):
        self._mode = Modes(value=value.lower())
        return self

    def format(self):
        raise NotImplementedError()

    def partition_by(self):
        return self

    def save(self, path: str) -> None:
        # TODO: Validate input for s3 bucket name
        assert isinstance(path, str), "`path` must be of type string"
        assert path[:2] == "s3", "`path` must be a S3 bucket"

        self._mode.command(self._session.conn, path)
        self._dataset._provider.invoke(
            execution_steps=self._dataset.execution_plan.execution_steps, prefix=path
        )

    def save_as_temp_table(self, table_name: str) -> None:
        # TODO: Locate where files are stored. ./cache/duckingit?
        assert isinstance(table_name, str), "`table_name` must be of type string"
        self._create_tmp_table(
            table_name=table_name, source=self._dataset.default_prefix
        )

        self._session._metadata[table_name] = self._dataset._query.sql


class Dataset:
    """
    The mapping between data objects in buckets and physical. If cache, then in memory
    locally.

    Unmanaged, managed datasets? Able to apply drop?

    Data source class to handle the connection between bucket and session?
    """

    _CACHE_PREFIX = ".cache/duckingit"

    def __init__(
        self,
        query: Query,
        execution_plan: Plan,
        session: "DuckSession",
    ) -> None:
        self._query = query
        self.execution_plan = execution_plan
        self._session = session

        self.default_prefix = f"{query.bucket}/{self._CACHE_PREFIX}/{query.hashed}"
        self._provider = Providers.AWS.klass(function_name=session._function_name)

    def __repr__(self) -> str:
        return f"Dataset<{self._query.sql}>"

    @property
    def write(self) -> DatasetWriter:
        return DatasetWriter(session=self._session, dataset=self)

    def drop(self) -> None:
        pass

    def show(self) -> duckdb.DuckDBPyRelation:
        self._provider.invoke(
            execution_steps=self.execution_plan.execution_steps,
            prefix=self.default_prefix,
        )

        return self._session.conn.sql(
            f"SELECT * FROM read_parquet(['{self.default_prefix}/*'])"
        )

    def print_shema(self):
        # SELECT * FROM parquet_schema('test.parquet');
        pass
