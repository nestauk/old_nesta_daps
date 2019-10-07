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
        {0: 0.906, 1: 0.674, 2: 0.12, 3: 0.548, 4: 0.287},
    ]

    assert dicts2sql_format(dicts, arrs) == [
        [
            {"doc_id": "a", "cluster_id": 0, "weight": 0.906},
            {"doc_id": "a", "cluster_id": 1, "weight": 0.674},
            {"doc_id": "a", "cluster_id": 3, "weight": 0.548},
            {"doc_id": "a", "cluster_id": 4, "weight": 0.287},
        ],
        [
            {"doc_id": "b", "cluster_id": 0, "weight": 0.906},
            {"doc_id": "b", "cluster_id": 1, "weight": 0.674},
            {"doc_id": "b", "cluster_id": 2, "weight": 0.11},
            {"doc_id": "b", "cluster_id": 4, "weight": 0.287},
        ],
        [{"doc_id": "c", "cluster_id": 0, "weight": 0.906}],
        [
            {"doc_id": "d", "cluster_id": 0, "weight": 0.906},
            {"doc_id": "d", "cluster_id": 1, "weight": 0.674},
            {"doc_id": "d", "cluster_id": 2, "weight": 0.12},
            {"doc_id": "d", "cluster_id": 3, "weight": 0.548},
            {"doc_id": "d", "cluster_id": 4, "weight": 0.287},
        ],
    ]
