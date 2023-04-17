import itertools
import hashlib
import typing as t
import uuid


T = t.TypeVar("T")


def flatten_list(_list: list[list[T]]) -> list[T]:
    return list(itertools.chain(*_list))


def split_list_in_chunks(_list: list[str], number_of_invokations: int) -> list[list]:
    # Must not invoke more functions than number of search queries
    if (size := len(_list)) < number_of_invokations:
        number_of_invokations = size

    k, m = divmod(len(_list), number_of_invokations)
    return [
        _list[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)]
        for i in range(number_of_invokations)
    ]


def create_md5_hash_string(string: str) -> str:
    return hashlib.md5(string.encode(), usedforsecurity=False).hexdigest()


def create_unique_name(prefix: str = "__duckingit") -> str:
    return f"{prefix}_{uuid.uuid1().hex[:6]}"
