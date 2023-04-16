from enum import Enum


class FileFormat(Enum):
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
