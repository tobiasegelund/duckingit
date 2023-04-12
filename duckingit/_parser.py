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
        scan_statements = self.expression.find_all(expr.Table)
        scan_statement = next(scan_statements).sql()

        # TODO: Update pattern for other filesystems or extensions
        pattern = r"\(.*?(s3://[^/]+/.+(/\*|\.parquet|\*)).*?\)"
        match = re.search(pattern, scan_statement)

        if match is None:
            raise InvalidFilesystem(
                "An acceptable filesystem, e.g. 's3://<BUCKET_NAME>/*', couldn't be \
found."
            )

        return match.group(1)

    @classmethod
    def _unify_query(cls, query: str) -> str:
        try:
            return sqlglot.transpile(query, read="duckdb")[0]
        except Exception as e:
            raise ParserError(e)
