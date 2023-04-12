import re
from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as expr
import sqlglot.planner as planner

from ._exceptions import InvalidFilesystem, ParserError
from ._encode import create_md5_hash_string


@dataclass
class Query:
    sql: str
    hashed: str
    expression: expr.Expression
    dag: planner.Plan

    _list_of_prefixes: list[str] | None = None

    @classmethod
    def parse(cls, query: str):
        query = cls._unify_query(query)

        expression = sqlglot.parse_one(query)
        dag = planner.Plan(expression)

        return cls(
            sql=query,
            hashed=create_md5_hash_string(query),
            expression=expression,
            dag=dag,
        )

    @property
    def list_of_prefixes(self) -> list[str]:
        if self._list_of_prefixes is None:
            return [self.source]
        return self._list_of_prefixes

    @list_of_prefixes.setter
    def list_of_prefixes(self, prefixes: list[str]) -> None:
        if not isinstance(prefixes, list):
            raise ValueError(f"{prefixes} must be a list of strings")
        self._list_of_prefixes = prefixes

    @property
    def bucket(self) -> str:
        pattern = r"s3:\/\/([A-Za-z0-9_-]+)"
        match = re.search(pattern, self.sql)

        if not match:
            raise ValueError("Couldn't find bucket name in query")
        # TODO: Make regex more generic
        return "s3://" + match.group(1)

    @property
    def source(self) -> str:
        # TODO: Update exceptions to user-defined exceptions
        _source = self.dag.root.source.sql()
        if _source[1:3] == "s3":
            return _source

        # TODO: Update pattern for other filesystems or extensions
        # What about single file?
        pattern = r"LIST_VALUE\((.*?)\)"
        match = re.findall(pattern, _source)

        if len(match) == 0:
            raise InvalidFilesystem(
                "An acceptable filesystem, e.g. 's3://<BUCKET_NAME>/*', couldn't be \
found."
            )

        return match[0]

    @classmethod
    def _unify_query(cls, query: str) -> str:
        try:
            return sqlglot.transpile(query, read="duckdb")[0]
        except Exception as e:
            raise ParserError(e)
