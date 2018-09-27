import mock
import pytest

import seed_csv_processing
from seed_csv_processing import extract_date
from seed_csv_processing import extract_year
from seed_csv_processing import geocode


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
    def test_invalid_month_returns_none(self):
        assert extract_date('Cat 12 2009') is None
        assert extract_date('2000-19-09') is None
        assert extract_date('20/4/2009') is None

    def test_invalid_day_returns_none(self):
        assert extract_date('Mar 38 2001') is None
        assert extract_date('2000-09-40') is None
        assert extract_date('5/32/2017') is None


class TestYearExtraction():
    def test_valid_year_extract(self):
        assert extract_year('2019') == '2019-01-01'
        assert extract_year('sometime in 2011') == '2011-01-01'
        assert extract_year('maybe 2019 or 2020') == '2019-01-01'

    def test_invalid_year_returns_none(self):
        assert extract_year('no year') is None
        assert extract_year('nan') is None
        assert extract_year('-') is None


class TestGeocoding():
    @staticmethod
    @pytest.fixture
    def mocked_osm_response():
        mocked_response = mock.Mock()
        mocked_response.json.return_value = [{'lat': '12.923432', 'lon': '-75.234569'}]
        return mocked_response

    def test_error_raised_when_arguments_missing(self):
        with pytest.raises(TypeError) as e:
            geocode()
        assert "Missing argument: query or city and country required" in str(e.value)

    @mock.patch('seed_csv_processing.requests.get')
    def test_request_includes_user_agent_in_header(self, mocked_request, mocked_osm_response):
        mocked_request.return_value = mocked_osm_response
        geocode('a')
        assert mocked_request.call_args[1]['headers'] == {'User-Agent': 'Nesta health data geocode'}

    @mock.patch('seed_csv_processing.requests.get')
    def test_url_correct_with_city_and_country(self, mocked_request, mocked_osm_response):
        mocked_request.return_value = mocked_osm_response
        geocode(city='london', country='UK')
        assert "https://nominatim.openstreetmap.org/search?city=london&country=UK&format=json" in mocked_request.call_args[0]

    @mock.patch('seed_csv_processing.requests.get')
    def test_url_correct_with_query(self, mocked_request, mocked_osm_response):
        mocked_request.return_value = mocked_osm_response
        geocode('my+place')
        assert "https://nominatim.openstreetmap.org/search?q=my+place&format=json" in mocked_request.call_args[0]

    @mock.patch('seed_csv_processing.requests.get')
    def test_none_returned_if_no_match(self, mocked_request):
        mocked_response = mock.Mock()
        mocked_response.json.return_value = []
        mocked_request.return_value = mocked_response
        assert geocode('nowhere') is None

    @mock.patch('seed_csv_processing.requests.get')
    def test_coordinates_extracted_from_json_with_one_result(self, mocked_request, mocked_osm_response):
        mocked_request.return_value = mocked_osm_response
        assert geocode('somewhere') == ['12.923432', '-75.234569']

    @mock.patch('seed_csv_processing.requests.get')
    def test_coordindates_of_first_result_extracted_from_json_with_multiple_results(self, mocked_request):
        mocked_response = mock.Mock()
        mocked_response.json.return_value = [
                {'lat': '123', 'lon': '456'},
                {'lat': '111', 'lon': '222'},
                {'lat': '777', 'lon': '888'}
                ]
        mocked_request.return_value = mocked_response
        assert geocode('nowhere') == ['123', '456']
