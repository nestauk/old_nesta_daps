import pandas as pd
import numpy as np
from collections import Counter
from numpy.testing import assert_array_equal
from pandas.testing import assert_frame_equal
import pytest

# from nesta.packages.dq.text import (string_length, string_counter)
from text import (string_length, string_counter)

@pytest.fixture
def data_string():
    return ['a', 'b', 'c',
            'ab', 'bc', 'ac', 'de', 'de',
                'abc', 'cde', 'efg']
@pytest.fixture
def data_string_array():
    return [ []

    ]

class TestStringDataQuality():

    def test_string_length(self, data_string):
        result = string_length(data_string)

        expected_data = np.array([1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3])

        assert_array_equal(result, expected_data)

    def test_string_counter(self, data_string):
        result =  dict(string_counter(data_string))


        expected_data = {'a': 1, 'ab': 1, 'abc': 1, 'ac': 1, 'b': 1,'bc': 1, 'c': 1, 'cde': 1, 'de': 2, 'efg': 1}

        assert all(v == result[k] for k,v in expected_data.items())
