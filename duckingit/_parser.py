import copy
import re
import typing as t
from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as exp

from duckingit._exceptions import InvalidFilesystem, ParserError
from duckingit._utils import create_hash_string, scan_source_for_prefixes


@dataclass
class Query:
    sql: str
    hashed: str
    ast: exp.Expression

    _list_of_prefixes: list[str] | None = None

    @classmethod
    def parse(cls, query: str):
        query = cls._unify_query(query)

        expression = sqlglot.parse_one(query, read="duckdb")
        # optimized_expression = optimizer.optimize(expression)

        return cls(
            sql=query,
            hashed=create_hash_string(query),
            ast=expression,
        )

    def replace(self, old: str, new: str) -> None:
        self.sql = self.sql.replace(old, new)

    @property
    def list_of_prefixes(self) -> list[str]:
        if self._list_of_prefixes is None:
            self._list_of_prefixes = scan_source_for_prefixes(source=self.source)
        return self._list_of_prefixes

    @property
    def tables(self) -> t.Generator:
        """Returns a generator that yields over table expressions, e.g. READ_PARQUET(VALUES(XX))"""
        yield from self.ast.find_all(exp.Table)

    @property
    def from_(self) -> t.Generator:
        """Returns a generator that yields over from expressions, e.g. FROM READ_PARQUET(VALUES(XX))"""
        yield from self.ast.find_all(exp.From)

    @property
    def bucket(self) -> str:
        """Returns the name of the bucket, e.g. s3://bucket-name-test

        Note that DuckDB uses S3 for GCP as well
        https://duckdb.org/docs/guides/import/s3_import.html
        """
        for table in self.tables:
            pattern = r"s3:\/\/([A-Za-z0-9_-]+)"
            match = re.search(pattern, str(table))

            if not match:
                continue

            return "s3://" + match.group(1)  # yield?

        raise ValueError("Not able to locate any bucket name in query")

    @property
    def source(self) -> str:
        """Returns the source of the table, e.g. s3://BUCKET_NAME/2023"""
        # TODO: Update exceptions to user-defined exceptions
        for table in self.tables:
            # TODO: Update pattern for other filesystems or extensions
            # What about single file?
            pattern = r"ARRAY\((.*?)\)"  # sqlglot bug, should be LIST_VALUE
            match = re.search(pattern, str(table))

            if not match:
                continue

            # TODO: Update to generator
            return match.group(1)

        raise InvalidFilesystem(
            "An acceptable filesystem, e.g. 's3://BUCKET_NAME/*', couldn't be found."
        )

    def copy(self):
        """Returns a deep copy of the object itself"""
        return copy.deepcopy(self)

    @classmethod
    def _unify_query(cls, query: str) -> str:
        try:
            return sqlglot.transpile(query, read="duckdb")[0]
        except Exception as e:
            raise ParserError(e) from e
