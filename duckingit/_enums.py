from enum import Enum


class Format(Enum):
    PARQUET = "parquet"
    JSON = "json"
    ORC = "orc"
    AVRO = "avro"
