import pytest

from seed_csv_processing import extract_date


class TestExtractDateSuccess():
    def test_string_date_pattern(self):
        assert extract_date('Sep 21 2017') == '2017-09-21'
        assert extract_date('Mar  1 2011') == '2011-03-01'
        assert extract_date('Apr  7 2009') == '2009-04-07'

    def test_dash_date_pattern(self):
        assert extract_date('2016-07-31') == '2016-07-31'
        assert extract_date('2010-12-01') == '2010-12-01'
        assert extract_date('2020-01-04') == '2020-01-04'

    def test_slash_date_pattern(self):
        assert extract_date('5/31/2020') == '2020-05-31'
        assert extract_date('11/1/2012') == '2012-11-01'
        assert extract_date('1/1/2010') == '2010-01-01'


class TestExtractDateFailure():
    def test_invalid_month_string(self):
        with pytest.raises(ValueError):
            extract_date('Cat 12 2009')

    def test_invalid_month_dash(self):
        with pytest.raises(ValueError):
            extract_date('2000-19-09')

    def test_invalid_month_slash(self):
        with pytest.raises(ValueError):
            extract_date('20/4/2009')

    def test_invalid_day_string(self):
        with pytest.raises(ValueError):
            extract_date('Mar 38 2001')

    def test_invalid_day_dash(self):
        with pytest.raises(ValueError):
            extract_date('2000-09-40')

    def test_invalid_day_slash(self):
        with pytest.raises(ValueError):
            extract_date('5/32/2017')
