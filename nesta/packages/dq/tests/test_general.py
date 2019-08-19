import pandas as pd
import numpy as np
from collections import Counter
from numpy.testing import assert_array_equal
from pandas.testing import assert_frame_equal
from pandas.api.types import CategoricalDtype

import pytest

# from nesta.packages.dq.text import (string_length, string_counter)
import sys
sys.path.insert(0, '/mnt/c/Users/aotubusen/Google Drive/Documents/DS Projects/nesta/nesta/packages/dq')
from general import (missing_values, missing_value_percentage_column_count, missing_value_count_pair_both, missing_value_count_pair_either)


@pytest.fixture
def data_dict():
    return {'a':[1,2,None, 'asd'],
            'b': [None, 2, 4, 'lo'],
            'c': [1,2,None,None]}

class TestGeneralDataQuality():

    def test_missing_values(self, data_dict):
        result = missing_values(data_dict)

        expected_result = pd.DataFrame({'field':['c', 'b', 'a'], 'frequency': [2, 1, 1]})

        assert_frame_equal(result, expected_result)

    def test_missing_value_percentage_column_count(self, data_dict):
        result = missing_value_percentage_column_count(data_dict)

        expected_result = [np.array([pd.Interval(-0.001, 10.0, closed='right'),
                             pd.Interval(10.0, 20.0, closed='right'),
                             pd.Interval(20.0, 30.0, closed='right'),
                             pd.Interval(30.0, 40.0, closed='right'),
                             pd.Interval(40.0, 50.0, closed='right'),
                             pd.Interval(50.0, 60.0, closed='right'),
                             pd.Interval(60.0, 70.0, closed='right'),
                             pd.Interval(70.0, 80.0, closed='right'),
                             pd.Interval(80.0, 90.0, closed='right'),
                             pd.Interval(90.0, 100.0, closed='right')]),
                             np.array([0, 0, 0, 2, 0, 0, 1, 0, 0, 0])]
        assert_array_equal(np.array(result['intervals']), expected_result[0])
        assert_array_equal(np.array(result['frequency']), expected_result[1])

    def test_missing_value_count_pair_both(self, data_dict):
        result = missing_value_count_pair_both(data_dict)

        expected_result = pd.DataFrame({
                                        'a': [1,0,1],
                                        'b': [0,1,0],
                                        'c': [1,0,2]}, index=['a','b','c'])

        assert_frame_equal(result, expected_result)

    def test_missing_value_count_pair_either(self, data_dict):
        result = missing_value_count_pair_either(data_dict)

        expected_result = pd.DataFrame({
                                        'a': [1,2,2],
                                        'b': [2,1,3],
                                        'c': [2,3,2]}, index=['a','b','c'])
