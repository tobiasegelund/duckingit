import re
import itertools

import duckdb
import sqlglot

from ._exceptions import WrongInvokationType, InvalidFilesystem, InvalidQueryFormat


class Planner:
    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self.conn = conn

    def scan_bucket(self, key: str) -> list[str]:
        if key[-1] != "*":
            return [key]

        # TODO: Count the number of files / size in each prefix to divide the workload better
        # TODO: Consider which of 2023/01/* or 2023/01* is most acceptable - a logic that can take both?
        glob_query = f"""
            SELECT DISTINCT
                CONCAT(REGEXP_REPLACE(file, '/[^/]+$', ''), '/*') AS prefix
            FROM GLOB('{key}')
            """

        try:
            prefixes = self.conn.sql(glob_query).fetchall()

        except RuntimeError:
            raise ValueError(
                "Please validate that the FROM statement in the query is correct."
            )

        return self._flatten_list(_list=prefixes)

    def _flatten_list(self, _list: list) -> list:
        return list(itertools.chain(*_list))

    def _split_list_in_chunks(
        self, _list: list[str], number_of_invokations: int
    ) -> list[list]:
        k, m = divmod(len(_list), number_of_invokations)
        return [
            _list[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)]
            for i in range(number_of_invokations)
        ]

    def find_key(self, query: str) -> str:
        # TODO: Update exceptions to user-defined exceptions
        scan_statements = sqlglot.parse_one(query).find_all(sqlglot.exp.Table)
        scan_statement = next(scan_statements).sql()

        # TODO: Update pattern for other filesystems or extensions
        pattern = r"\(.*?(s3://[^/]+/.+(/\*|\.parquet|\*)).*?\)"
        match = re.search(pattern, scan_statement)

        if match is None:
            # TODO: Validate that the prefix/file exists else raise Exception
            raise InvalidFilesystem(
                "An acceptable filesystem, e.g. 's3://<BUCKET_NAME>/*' couldn't be \
found. Did you try to run local files?"
            )

        return match.group(1)

    def update_query(self, query: str, list_of_prefixes: list[str]) -> str:
        sub = f"read_parquet({list_of_prefixes})"

        query_upd = re.sub(r"(scan_parquet|read_parqet)\([^)]*\)", sub, query)

        return query_upd

    def plan(self, query: str, invokations: int | str) -> list[str]:
        query = QueryParser.parse(query)

        key = self.find_key(query)
        list_of_prefixes = self.scan_bucket(key=key)

        if isinstance(invokations, str):
            if invokations != "auto":
                raise WrongInvokationType(
                    f"The number of invokations can only be 'auto' or an integer. \
{invokations} was provided."
                )
            invokations = len(list_of_prefixes)

        # TODO: Heuristic to divide the workload between the invokations based on size of prefixes / number of files etc.
        # Or based on some deeper analysis of the query?
        list_of_chunks_of_prefixes = self._split_list_in_chunks(
            list_of_prefixes, number_of_invokations=invokations
        )

        updated_queries = list()
        for chunk in list_of_chunks_of_prefixes:
            query_upd = self.update_query(query=query, list_of_prefixes=chunk)
            updated_queries.append(query_upd)
        return updated_queries


class QueryParser:
    """Class to unify queries

    TODO:
        - Validate that S3 etc. is in the query?
        - Add date as column based on prefix?
    """

    @classmethod
    def verify_query(cls, query: str) -> str:
        try:
            sqlglot.parse_one(query)
        except Exception as e:
            raise InvalidQueryFormat(e)
        return query

    @classmethod
    def apply_lower_case(cls, query: str) -> str:
        # TODO: Logic to make sure uppercase names are still in place
        return query.lower()

    @classmethod
    def parse(cls, query: str) -> str:
        query = cls.verify_query(query)
        # query = cls.apply_lower_case(query)

        return query


# class Optimizer:
#     pass

#     def analyze_query(self, query: str):
#         pass
