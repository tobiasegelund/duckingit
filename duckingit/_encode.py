import hashlib


def create_md5_hash_string(string: str) -> str:
    return hashlib.md5(string.encode(), usedforsecurity=False).hexdigest()
