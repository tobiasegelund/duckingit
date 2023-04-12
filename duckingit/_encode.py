import hashlib
from enum import Enum


class Format(Enum):
    PARQUET = "parquet"
    JSON = "json"


#     CSV = "csv"
#     ORC = "orc"
#     AVRO = "avro"


def create_md5_hash_string(string: str) -> str:
    return hashlib.md5(string.encode(), usedforsecurity=False).hexdigest()
