from nesta.packages.health_data.process_nih import _extract_date


class TestExtractDateSuccess():
    def test_string_date_pattern(self):
        assert _extract_date('Sep 21 2017') == '2017-09-21'
        assert _extract_date('Mar  1 2011') == '2011-03-01'
        assert _extract_date('Apr  7 2009') == '2009-04-07'
        assert _extract_date('January 2016') == '2016-01-01'
        assert _extract_date('Oct 2014') == '2014-10-01'
        assert _extract_date('2015') == '2015-01-01'
        assert _extract_date('6 April 2018') == '2018-04-06'
        assert _extract_date('8 Dec, 2010') == '2010-12-08'

    def test_dash_date_pattern(self):
        assert _extract_date('2016-07-31') == '2016-07-31'
        assert _extract_date('2010-12-01') == '2010-12-01'
        assert _extract_date('2020-01-04') == '2020-01-04'

    def test_slash_date_pattern(self):
        assert _extract_date('5/31/2020') == '2020-05-31'
        assert _extract_date('11/1/2012') == '2012-11-01'
        assert _extract_date('1/1/2010') == '2010-01-01'
        assert _extract_date('2000/12/01') == '2000-12-01'
        assert _extract_date('1999/04/20') == '1999-04-20'

    def test_invalid_month_returns_year(self):
        assert _extract_date('Cat 12 2009') == '2009-01-01'
        assert _extract_date('2000-19-09') == '2000-01-01'
        assert _extract_date('20/4/2009') == '2009-01-01'

    def test_invalid_day_returns_year(self):
        assert _extract_date('Mar 38 2001') == '2001-01-01'
        assert _extract_date('2000-09-40') == '2000-01-01'
        assert _extract_date('5/32/2017') == '2017-01-01'

    def test_valid_year_extract(self):
        assert _extract_date('2019') == '2019-01-01'
        assert _extract_date('sometime in 2011') == '2011-01-01'
        assert _extract_date('maybe 2019 or 2020') == '2019-01-01'

    def test_invalid_year_returns_none(self):
        assert _extract_date('no year') is None
        assert _extract_date('nan') is None
        assert _extract_date('-') is None
