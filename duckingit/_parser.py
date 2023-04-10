import re
import hashlib

import sqlglot

from ._exceptions import InvalidFilesystem, InvalidQueryFormat


class QueryParser:
    """Class to unify queries

    TODO:
        - Validate that S3 etc. is in the query?
        - Add date as column based on prefix?
    """

    @classmethod
    def hash_query(cls, query: str) -> str:
        return hashlib.md5(query.encode()).hexdigest()

    @classmethod
    def find_bucket(cls, query: str) -> str:
        pattern = r"s3:\/\/([A-Za-z0-9_-]+)"
        match = re.search(pattern, query)

        if not match:
            raise ValueError("Couldn't find bucket name in query")
        # TODO: Make regex more generic
        return "s3://" + match.group(1)

    @classmethod
    def find_key(cls, query: str) -> str:
        # TODO: Update exceptions to user-defined exceptions
        scan_statements = sqlglot.parse_one(query).find_all(sqlglot.exp.Table)
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
    def verify_query(cls, query: str) -> str:
        try:
            sqlglot.parse_one(query)
        except Exception as e:
            raise InvalidQueryFormat(e)
        return query

    @classmethod
    def remove_newlines_and_tabs(cls, query: str) -> str:
        return " ".join(query.split())

    @classmethod
    def apply_lower_case(cls, query: str) -> str:
        # TODO: Logic to make sure uppercase names are still in place
        return query.lower()

    @classmethod
    def parse(cls, query: str) -> str:
        query = cls.verify_query(query)
        query = cls.remove_newlines_and_tabs(query)
        # query = cls.apply_lower_case(query)

        return query
