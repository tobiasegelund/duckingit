import pytest

from duckingit._utils import (
    flatten_list,
    ensure_iterable,
    create_md5_hash_string,
    split_list_in_chunks,
)


def test_flatten_list():
    input = [[1, 2], [3, 4]]

    expected = [1, 2, 3, 4]
    got = flatten_list(input)

    assert got == expected


@pytest.mark.parametrize(
    "input, expected",
    [(1, [1]), ([1, 2], [1, 2]), (None, []), ("a", "a"), (b"a", b"a")],
)
def test_ensure_iterable(input, expected):
    got = ensure_iterable(input)

    assert got == expected


@pytest.mark.parametrize(
    "input, invokations, expected",
    [
        (
            ["SELECT * FROM table1", "SELECT * FROM table2"],
            3,
            [["SELECT * FROM table1"], ["SELECT * FROM table2"]],
        ),
        (
            ["SELECT * FROM table1", "SELECT * FROM table2"],
            1,
            [["SELECT * FROM table1", "SELECT * FROM table2"]],
        ),
    ],
)
def test_split_list_in_chunks(input, invokations, expected):
    got = split_list_in_chunks(input, invokations)

    assert got == expected
