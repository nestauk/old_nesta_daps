import pytest

import numpy as np

from nesta.packages.nlp_utils.embed_clustering import vals2arr
from nesta.packages.nlp_utils.embed_clustering import clustering
from nesta.packages.nlp_utils.embed_clustering import filter_arr


def test_vals2arr():
    arr = np.random.rand(3, 2)
    d = {i: arr[i] for i in range(arr.shape[0])}
    _, arrays = vals2arr(d)
    assert arrays.shape == arr.shape


def test_clustering():
    arr = np.random.rand(1000, 3)
    # Test reproducibility
    gmm_model_v1 = clustering(arr)
    gmm_model_v2 = clustering(arr)
    np.testing.assert_array_(gmm_model_v1.predict_proba(arr), gmm_model_v2.predict_proba(arr))


def test_filter_arr():
    arr = np.array([0.906, 0.674, 0.01, 0.548, 0.287])
    assert filter_arr(arr) == {0: 0.906, 1: 0.674, 3: 0.548, 4: 0.287}
