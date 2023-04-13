import hashlib
from enum import Enum


class Format(Enum):
    PARQUET = "parquet"
    JSON = "json"

    #     CSV = "csv"
    #     ORC = "orc"
    #     AVRO = "avro"

    @property
    def read_expression(self) -> str:
        expressions = {
            self.PARQUET: "READ_PARQUET(LIST_VALUE({}))",
            self.JSON: "READ_JSON(LIST_VALUE({}))",
        }
        return expressions[self]

    @property
    def scan_expression(self) -> str:
        expressions = {
            self.PARQUET: "SCAN_PARQUET(LIST_VALUE({}))",
            self.JSON: "READ_JSON_AUTO(LIST_VALUE({}))",
        }
        return expressions[self]


def create_md5_hash_string(string: str) -> str:
    return hashlib.md5(string.encode(), usedforsecurity=False).hexdigest()
