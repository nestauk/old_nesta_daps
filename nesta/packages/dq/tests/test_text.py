import pandas as pd
import numpy as np
from collections import Counter
from numpy.testing import assert_array_equal

import pytest

# from nesta.packages.dq.text import (string_length, string_counter)
import sys
sys.path.insert(0, '/mnt/c/Users/aotubusen/Documents/DS Projects/nesta_dq/nesta/nesta/packages/dq/')
from text import (string_length, string_counter)

@pytest.fixture
def data_string():
    return ['a', 'b', 'c',
            'ab', 'bc', 'ac', 'de', 'de',
                'abc', 'cde', 'efg']

class TestStringDataQuality():

    def test_string_length(self, data_string):
        result = string_length(data_string)

        expected_data = np.array([1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3])

        assert_array_equal(result, expected_data)

    def test_string_counter(self, data_string):
        output =  string_counter(data_string)
        result = dict(zip(output['Text'], output[0]))


        expected_data = {'de': 2,
                         'ab': 1,
                         'ac': 1,
                         'c': 1,
                         'a': 1,
                         'abc': 1,
                         'bc': 1,
                         'cde': 1,
                         'efg': 1,
                         'b': 1}
        assert result == expected_data
