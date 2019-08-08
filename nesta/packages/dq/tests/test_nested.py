import pandas as pd
import numpy as np
from collections import Counter
from numpy.testing import assert_array_equal

import pytest

# from nesta.packages.dq.text import (string_length, string_counter)
import sys
sys.path.insert(0, '/mnt/c/Users/aotubusen/Google Drive/Documents/DS Projects/nesta/nesta/packages/dq')
from nested import (array_length, word_arrays, word_array_calc)

@pytest.fixture
def data_list():
    return [['a', 'b', 'c'],
            ['ab', 'bc', 'ac', 'de', 'de'],
            None, None,
                ['abc', 'cde', 'efg'], None]

class TestNestedDataQuality():

    def test_array_length(self, data_list):
        result = array_length(data_list)

        expected_data = np.array([3,5, np.nan, np.nan, 3, np.nan])

        assert_array_equal(result, expected_data)

    def test_word_arrays(self, data_list):
        result = word_arrays(data_list)

        expected_data = np.array([['a', 'b', 'c'],
                ['ab', 'bc', 'ac', 'de', 'de'],
                ['abc', 'cde', 'efg']]
        )
        assert_array_equal(result, expected_data)

    def test_word_array_calc(self, data_list):
        result_1 = word_array_calc(data_list, 'word_length')
        result_2 = dict(word_array_calc(data_list, 'count'))

        expected_data_1 = np.array([1,1,1,2,2,2,2,2,3,3,3])
        expected_data_2 = Counter({'a': 1,
         'b': 1,
         'c': 1,
         'ab': 1,
         'bc': 1,
         'ac': 1,
         'de': 2,
         'abc': 1,
         'cde': 1,
         'efg': 1})
        assert_array_equal(result_1, expected_data_1)

        assert all(v == result_2[k] for k,v in expected_data_2.items())



    # def test_string_counter(self, data_string):
    #     result =  dict(string_counter(data_string))
    #
    #
    #     expected_data = {'a': 1, 'ab': 1, 'abc': 1, 'ac': 1, 'b': 1,'bc': 1, 'c': 1, 'cde': 1, 'de': 2, 'efg': 1}
    #
    #     assert all(v == result[k] for k,v in expected_data.items())
