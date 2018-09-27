import mock
import pandas as pd
import pytest
from io import StringIO

from seed_csv_processing import extract_date
from seed_csv_processing import extract_year
from seed_csv_processing import fix_dates
from seed_csv_processing import geocode
from seed_csv_processing import geocode_dataframe


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


class TestDateFixDataFrame():
    @staticmethod
    @pytest.fixture
    def test_dataframe():
        data = pd.DataFrame({
            'start_date': ['5/30/1999', 'nan', '2012', 'cat'],
            'end_date': ['Apr  7 2009', 'October', 'maybe 2020', 'ongoing']
            })
        return data

    def test_invalid_start_dates_are_none(self, test_dataframe):
        fixed_dates = fix_dates(test_dataframe)
        assert fixed_dates.start_date[1] is None
        assert fixed_dates.start_date[3] is None
        assert False  # these tests need to be re-written to mock the underlying functionality

    def test_invalid_end_dates_are_none(self, test_dataframe):
        fixed_dates = fix_dates(test_dataframe)
        assert fixed_dates.end_date[1] is None
        assert fixed_dates.end_date[3] is None

    def test_start_date_extraction_succeeds(self, test_dataframe):
        fixed_dates = fix_dates(test_dataframe)
        assert fixed_dates.start_date[0] == '1999-05-30'

    def test_end_date_extraction_succeeds(self, test_dataframe):
        fixed_dates = fix_dates(test_dataframe)
        assert fixed_dates.end_date[0] == '2009-04-07'

    def test_start_date_year_extraction_when_date_extraction_fails(self, test_dataframe):
        fixed_dates = fix_dates(test_dataframe)
        assert fixed_dates.start_date[2] == '2012-01-01'

    def test_end_date_year_extraction_when_date_extraction_fails(self, test_dataframe):
        fixed_dates = fix_dates(test_dataframe)
        assert fixed_dates.end_date[2] == '2020-01-01'


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
        assert geocode('somewhere') == {'lat': '12.923432', 'lon': '-75.234569'}

    @mock.patch('seed_csv_processing.requests.get')
    def test_coordindates_of_first_result_extracted_from_json_with_multiple_results(self, mocked_request):
        mocked_response = mock.Mock()
        mocked_response.json.return_value = [
                {'lat': '123', 'lon': '456'},
                {'lat': '111', 'lon': '222'},
                {'lat': '777', 'lon': '888'}
                ]
        mocked_request.return_value = mocked_response
        assert geocode('best match') == {'lat': '123', 'lon': '456'}


class TestGeocodeDataFrame():
    @staticmethod
    @pytest.fixture
    def test_dataframe():
        data = pd.DataFrame({
            'index': [0, 1, 2, 3, 4, 5, 6],
            'city': ['London', 'Brussels', 'London', 'Sheffield', 'Manchester', 'Jamaica', 'London'],
            'country': ['UK', 'Belgium', 'United Kingdom', 'United Kingdom', 'UK', 'United States', 'UK']
            })
        return data

    def test_merge_with_existing_file(self, test_dataframe):
        with StringIO() as existing_file:
            existing_data = pd.DataFrame({
                'city': ['London', 'Sheffield', 'Brussels'],
                'country': ['UK', 'United Kingdom', 'Belgium'],
                'coordinates': [{'lat': 1.4, 'lon': 2.4}, {'lat': 1.3, 'lon': 2.3}, {'lat': 1.2, 'lon': 2.2}]
                })
            existing_data.to_json(existing_file, orient='records')
            merged_dataframes = geocode_dataframe(test_dataframe, existing_file=existing_file.getvalue())
        assert merged_dataframes.coordinates[0] == {'lat': 1.4, 'lon': 2.4}
        assert merged_dataframes.coordinates[1] == {'lat': 1.2, 'lon': 2.2}
        assert merged_dataframes.coordinates[3] == {'lat': 1.3, 'lon': 2.3}

    def test_request_exception_doesnt_crash_process(self):
        pass

    def test_fall_back_to_query_method(self):
        pass

    def test_duplicates_are_only_geocoded_once(self):
        pass
