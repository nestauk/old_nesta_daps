import pytest

import numpy as np
from nesta.packages.format_utils.listtools import flatten_lists
from nesta.packages.format_utils.listtools import dicts2sql_format


def test_flatten_lists():
    nested_list = [["a"], ["b"], ["c"]]
    assert ["a", "b", "c"] == flatten_lists(nested_list)


def test_dicts2sql_format():
    dicts = {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9], "d": [10, 11, 12]}
    arrs = [
        {0: 0.906, 1: 0.674, 3: 0.548, 4: 0.287},
        {0: 0.906, 1: 0.674, 2: 0.11, 4: 0.287},
        {0: 0.906},
        {0: 0.906, 1: 0.674, 2: 0.12, 3: 0.548, 4: 0.287}
    ]

    assert dicts2sql_format(dicts, arrs) == [
        [("a", 0, 0.906), ("a", 1, 0.674), ("a", 3, 0.548), ("a", 4, 0.287)],
        [("b", 0, 0.906), ("b", 1, 0.674), ("b", 2, 0.11), ("b", 4, 0.287)],
        [("c", 0, 0.906)],
        [
            ("d", 0, 0.906),
            ("d", 1, 0.674),
            ("d", 2, 0.12),
            ("d", 3, 0.548),
            ("d", 4, 0.287)
        ]
    ]
