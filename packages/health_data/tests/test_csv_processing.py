import pytest

from seed_csv_processing import extract_date
from seed_csv_processing import extract_year


class TestExtractDateSuccess():
    def test_string_date_pattern(self):
        assert extract_date('Sep 21 2017') == '2017-09-21'
        assert extract_date('Mar  1 2011') == '2011-03-01'
        assert extract_date('Apr  7 2009') == '2009-04-07'
        assert extract_date('January 2016') == '2016-01-01'
        assert extract_date('Oct 2014') == '2014-10-01'
        assert extract_date('2015') == '2015-01-01'
        assert extract_date('6 April 2018') == '2018-04-06'
        assert extract_date('8 Dec, 2010') == '2010-12-08'

    def test_dash_date_pattern(self):
        assert extract_date('2016-07-31') == '2016-07-31'
        assert extract_date('2010-12-01') == '2010-12-01'
        assert extract_date('2020-01-04') == '2020-01-04'

    def test_slash_date_pattern(self):
        assert extract_date('5/31/2020') == '2020-05-31'
        assert extract_date('11/1/2012') == '2012-11-01'
        assert extract_date('1/1/2010') == '2010-01-01'
        assert extract_date('2000/12/01') == '2000-12-01'
        assert extract_date('1999/04/20') == '1999-04-20'


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


class TestYearExtraction():
    def test_valid_year_extract(self):
        assert extract_year('2019') == '2019-01-01'
        assert extract_year('sometime in 2011') == '2011-01-01'
        assert extract_year('maybe 2019 or 2020') == '2019-01-01'

    def test_invalid_year_returns_none(self):
        assert extract_year('no year') is None
        assert extract_year('nan') is None
        assert extract_year('-') is None
