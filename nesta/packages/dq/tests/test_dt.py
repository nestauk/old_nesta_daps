import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal
import pytest

from nesta.packages.dq.dt import (month_year_distribution, year_distribution,
        calendar_day_distribution)


@pytest.fixture
def data():
    return pd.to_datetime(
                [
                '2011-02-01', '2011-03-02', '2011-04-03', '2011-05-04',
                '2011-06-05', '2011-07-06', '2011-08-07', '2011-09-08',
                '2011-10-09', '2011-11-10', '2011-12-11', '2012-01-12',
                '2012-01-13', '2012-02-14', '2012-03-15', '2012-04-16',
                '2012-05-16', '2012-06-16', '2012-07-16', '2012-08-16',
                '2012-09-16', '2012-10-16', '2012-11-16', '2012-12-31',
                ]
            )

class TestDateTimeDataQuality():

    def test_month_year_distribution(self, data):
        result = month_year_distribution(data)

        expected_data = pd.DataFrame(
                index=list(range(1, 13)),
                data={
                    2011: [0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                    2012: [2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                    }
                )
        expected_data.index.name = 'Month'
        expected_data.columns.name = 'Year'

        assert_frame_equal(result, expected_data, check_like=True)

    def test_year_distribution(self, data):
        result = year_distribution(data)

        expected_data = pd.DataFrame(
                data={'Year': [2011, 2012], 'Frequency': [11, 13]}
                )

        assert_frame_equal(result, expected_data, check_like=True)

    def test_calendar_day_distribution(self, data):
        result = calendar_day_distribution(data)

        expected_data = pd.DataFrame(
                data={
                    'Calendar Day': [
                        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 31],
                    'Frequency': [
                        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 8, 1],
                    }
                )

        assert_frame_equal(result, expected_data, check_like=True)
