import typing as t
from enum import Enum

import duckdb

from duckingit._config import CACHE_PREFIX
from duckingit._controller import Controller
from duckingit._exceptions import DatasetExistError
from duckingit._planner import Plan
from duckingit._utils import scan_source_for_files

if t.TYPE_CHECKING:
    from duckingit._session import DuckSession


class Modes(Enum):
    """A collection of modes to apply when writing

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
        def append(source: str):
            raise NotImplementedError()

        def overwrite(source: str):
            # TODO: Drop data at source
            pass

        def write(source: str) -> None:
            files = scan_source_for_files(source=source)
            if len(files) > 0:
                raise DatasetExistError(f"Table with name `{source}` already exists!")

        _funcs = {
            self.APPEND: append,
            self.OVERWRITE: overwrite,
            self.WRITE: write,
        }

        return _funcs[self]  # type: ignore


class DatasetWriter:
    _mode: Modes = Modes.WRITE

    def __init__(self, session: "DuckSession", dataset: "Dataset") -> None:
        self._session = session
        self.ds = dataset

    def mode(self, value: str = "write"):
        self._mode = Modes(value=value.lower())
        return self

    def format(self):
        raise NotImplementedError()

    def partition_by(self):
        raise NotImplementedError()

    def save(self, path: str) -> None:
        """Writes the data to a specified source, e.g. S3 Bucket

        Note that the `path` must be a path to a storage solution on the provider.

        Args:
            path, str: The path to store data objects

        Example:
            >>> dataset = session.sql(query)
            >>> dataset.write.save(path="s3://BUCKET_NAME/test")
        """
        assert isinstance(path, str), "`path` must be of type string"
        assert path[:2] in ["s3"], "`path` must be a S3 bucket"

        if path[-1] == "/":
            path = path[:-1]

        self._mode.command(path)

        self.ds._execute_plan(prefix=path)


class Dataset:
    """"""

    def __init__(
        self,
        execution_plan: Plan,
        session: "DuckSession",
    ) -> None:
        self.execution_plan = execution_plan
        self._session = session

        self._set_controller()

        self.default_prefix = f"{execution_plan.query.bucket}/{CACHE_PREFIX}"

    def __repr__(self) -> str:
        return f"""Dataset<SQL=`{self.execution_plan.query.sql}` | HASH_VALUE=`{self.execution_plan.query.hashed}`>"""

    def _set_controller(self) -> None:
        self._controller = Controller(session=self._session)

    def _execute_plan(self, prefix: str = ""):
        self._controller.execute_plan(
            execution_plan=self.execution_plan,
            prefix=prefix,
            default_prefix=self.default_prefix,
        )

    @property
    def stored_objects(self) -> list[str]:
        return list(
            self.default_prefix + "/" + task.subquery_hashed + ".parquet"
            for task in self.execution_plan.root.tasks
        )

    @property
    def write(self) -> DatasetWriter:
        return DatasetWriter(session=self._session, dataset=self)

    def drop(self) -> None:
        raise NotImplementedError()

    def show(self) -> duckdb.DuckDBPyRelation:
        self._execute_plan(prefix=self.default_prefix)

        return self._session.conn.sql(f"SELECT * FROM READ_PARQUET({self.stored_objects})")

    def createOrReplaceTempTable(self, table_name: str) -> None:
        """Writes or replaces a temporary table locally

        Be cautious that it saves a local copy of the data in memory, thus you may run
        out of memory trying to load in the dataset.

        Args:
            table_name, str: The name of the table to create

        Example:
            >>> dataset = session.sql(query)
            >>> dataset.createOrReplaceTempTable(table_name="test")
        """
        self._execute_plan()

        self._session.conn.sql(
            f"""
            CREATE OR REPLACE TEMP TABLE {table_name}
            AS
            SELECT * FROM READ_PARQUET({self.stored_objects})
            """
        )

        self._session.metadata[table_name] = self.execution_plan.query.sql

    def createTempTable(self, table_name: str) -> None:
        """Writes a temporary table locally

        Be cautious that it saves a local copy of the data in memory, thus you may run
        out of memory trying to load in the dataset.

        Args:
            table_name, str: The name of the table to create

        Example:
            >>> dataset = session.sql(query)
            >>> dataset.saveAsTempTable(table_name="test")
        """
        assert isinstance(table_name, str), "`table_name` must be of type string"
        self._execute_plan()

        self._session.conn.sql(
            f"""
            CREATE TEMP TABLE {table_name}
            AS
            SELECT * FROM READ_PARQUET({self.stored_objects})
            """
        )

        self._session.metadata[table_name] = self.execution_plan.query.sql
