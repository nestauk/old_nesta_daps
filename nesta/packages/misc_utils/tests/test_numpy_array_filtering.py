import pytest
import numpy as np

from nesta.packages.misc_utils.np_utils import arr2dic


def test_array_filtering():
    arr = np.array([0.906, 0.674, 0.01, 0.548, 0.287])
    assert arr2dic(arr) == {0: 0.906, 1: 0.674, 2: 0.01, 3: 0.548, 4: 0.287}
    assert arr2dic(arr, thresh=0.1) == {0: 0.906, 1: 0.674, 3: 0.548, 4: 0.287}
