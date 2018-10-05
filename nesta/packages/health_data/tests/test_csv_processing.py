from io import StringIO
import mock
import pandas as pd
import pytest
import requests
import time

from nesta.packages.health_data.seed_csv_processing import extract_date
from nesta.packages.health_data.seed_csv_processing import extract_year
from nesta.packages.health_data.seed_csv_processing import fix_dates
from nesta.packages.health_data.seed_csv_processing import geocode
from nesta.packages.health_data.seed_csv_processing import geocode_dataframe


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
    @mock.patch('nesta.packages.health_data.seed_csv_processing.extract_date')
    @mock.patch('nesta.packages.health_data.seed_csv_processing.extract_year')
    def test_start_and_end_date_extraction_succeeds(self, mocked_year, mocked_date):
        test_dataframe = pd.DataFrame({
                'start_date': ['5/30/1999', '2012-07-14'],
                'end_date': ['Apr  7 2009', 'January 2000']
                })
        mocked_date.side_effect = ['1999-05-30', '2009-04-07', '2012-07-14', '2000-01-01']

        fixed_dates = fix_dates(test_dataframe)

        expected_start_dates = pd.Series(['1999-05-30', '2012-07-14'])
        expected_end_dates = pd.Series(['2009-04-07', '2000-01-01'])
        expected_date_calls = [mock.call('5/30/1999'),
                               mock.call('Apr  7 2009'),
                               mock.call('2012-07-14'),
                               mock.call('January 2000')
                               ]

        assert fixed_dates.start_date.equals(expected_start_dates)
        assert fixed_dates.end_date.equals(expected_end_dates)
        mocked_date.assert_has_calls(expected_date_calls)
        mocked_year.assert_not_called()

    @mock.patch('nesta.packages.health_data.seed_csv_processing.extract_date')
    @mock.patch('nesta.packages.health_data.seed_csv_processing.extract_year')
    def test_year_extraction_when_date_extraction_fails(self, mocked_year, mocked_date):
        test_dataframe = pd.DataFrame({
                'start_date': ['5/1999', 'hopefully 2012'],
                'end_date': ['Apri 2009', '2000 or 2001']
                })
        mocked_date.return_value = None
        mocked_year.side_effect = ['1999-01-01', '2009-01-01', '2012-01-01', '2000-01-01']

        fixed_dates = fix_dates(test_dataframe)

        expected_start_dates = pd.Series(['1999-01-01', '2012-01-01'])
        expected_end_dates = pd.Series(['2009-01-01', '2000-01-01'])
        expected_year_calls = [mock.call('5/1999'),
                               mock.call('Apri 2009'),
                               mock.call('hopefully 2012'),
                               mock.call('2000 or 2001')
                               ]

        assert fixed_dates.start_date.equals(expected_start_dates)
        assert fixed_dates.end_date.equals(expected_end_dates)
        assert mocked_date.call_count == 4
        mocked_year.assert_has_calls(expected_year_calls)

    @mock.patch('nesta.packages.health_data.seed_csv_processing.extract_date')
    @mock.patch('nesta.packages.health_data.seed_csv_processing.extract_year')
    def test_start_and_end_date_are_none_if_no_match(self, mocked_year, mocked_date):
        test_dataframe = pd.DataFrame({
                'start_date': ['100/10', 'hopefully'],
                'end_date': ['April', 'S000']
                })
        mocked_date.return_value = None
        mocked_year.return_value = None

        fixed_dates = fix_dates(test_dataframe)

        expected_start_dates = pd.Series([None, None])
        expected_end_dates = pd.Series([None, None])
        expected_calls = [mock.call('100/10'),
                          mock.call('April'),
                          mock.call('hopefully'),
                          mock.call('S000')
                          ]

        assert fixed_dates.start_date.equals(expected_start_dates)
        assert fixed_dates.end_date.equals(expected_end_dates)
        mocked_date.assert_has_calls(expected_calls)
        mocked_year.assert_has_calls(expected_calls)

    @mock.patch('nesta.packages.health_data.seed_csv_processing.extract_date')
    def test_original_dates_moved_to_renamed_columns(self, mocked_date):
        test_dataframe = pd.DataFrame({
                'start_date': ['5/30/1999', '2012-07-14'],
                'end_date': ['Apr  7 2009', 'January 2000']
                })
        mocked_date.side_effect = ['1999-05-30', '2009-04-07', '2012-07-14', '2000-01-01']

        fixed_dates = fix_dates(test_dataframe)

        expected_start_dates = pd.Series(['5/30/1999', '2012-07-14'])
        expected_end_dates = pd.Series(['Apr  7 2009', 'January 2000'])

        assert fixed_dates.original_start_date.equals(expected_start_dates)
        assert fixed_dates.original_end_date.equals(expected_end_dates)


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

    @mock.patch('nesta.packages.health_data.seed_csv_processing.requests.get')
    def test_request_includes_user_agent_in_header(self, mocked_request, mocked_osm_response):
        mocked_request.return_value = mocked_osm_response
        geocode('a')
        assert mocked_request.call_args[1]['headers'] == {'User-Agent': 'Nesta health data geocode'}

    @mock.patch('nesta.packages.health_data.seed_csv_processing.requests.get')
    def test_url_correct_with_city_and_country(self, mocked_request, mocked_osm_response):
        mocked_request.return_value = mocked_osm_response
        geocode(city='london', country='UK')
        assert "https://nominatim.openstreetmap.org/search?city=london&country=UK&format=json" in mocked_request.call_args[0]

    @mock.patch('nesta.packages.health_data.seed_csv_processing.requests.get')
    def test_url_correct_with_query(self, mocked_request, mocked_osm_response):
        mocked_request.return_value = mocked_osm_response
        geocode('my+place')
        assert "https://nominatim.openstreetmap.org/search?q=my+place&format=json" in mocked_request.call_args[0]

    @mock.patch('nesta.packages.health_data.seed_csv_processing.requests.get')
    def test_none_returned_if_no_match(self, mocked_request):
        mocked_response = mock.Mock()
        mocked_response.json.return_value = []
        mocked_request.return_value = mocked_response
        assert geocode('nowhere') is None

    @mock.patch('nesta.packages.health_data.seed_csv_processing.requests.get')
    def test_coordinates_extracted_from_json_with_one_result(self, mocked_request, mocked_osm_response):
        mocked_request.return_value = mocked_osm_response
        assert geocode('somewhere') == {'lat': '12.923432', 'lon': '-75.234569'}

    @mock.patch('nesta.packages.health_data.seed_csv_processing.requests.get')
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
        df = pd.DataFrame({
                'index': [0, 1, 2],
                'city': ['London', 'Sheffield', 'Brussels'],
                'country': ['UK', 'United Kingdom', 'Belgium'],
                })
        return df

    def test_merge_with_existing_file(self, test_dataframe):
        with StringIO() as existing_file:
            existing_data = pd.DataFrame({
                'city': ['London', 'Brussels', 'Sheffield'],
                'country': ['UK', 'Belgium', 'United Kingdom'],
                'coordinates': [{'lat': 1.4, 'lon': 2.4}, {'lat': 1.3, 'lon': 2.3}, {'lat': 1.2, 'lon': 2.2}]
                })
            existing_data.to_json(existing_file, orient='records')
            merged_dataframes = geocode_dataframe(test_dataframe, existing_file=existing_file.getvalue())

        assert merged_dataframes.coordinates[0] == {'lat': 1.4, 'lon': 2.4}
        assert merged_dataframes.coordinates[1] == {'lat': 1.2, 'lon': 2.2}
        assert merged_dataframes.coordinates[2] == {'lat': 1.3, 'lon': 2.3}

    @mock.patch('nesta.packages.health_data.seed_csv_processing.geocode')
    def test_underlying_geocoding_function_called_with_city_country(self, mocked_geocode, test_dataframe):
        mocked_geocode.side_effect = ['cat', 'dog', 'squirrel']

        with StringIO() as temp_out:
            geocoded_dataframe = geocode_dataframe(test_dataframe, out_file=temp_out)

        expected_dataframe = pd.DataFrame({
                'index': [0, 1, 2],
                'city': ['London', 'Sheffield', 'Brussels'],
                'country': ['UK', 'United Kingdom', 'Belgium'],
                'coordinates': ['cat', 'dog', 'squirrel']
                })
        expected_calls = [mock.call(city='London', country='UK'),
                          mock.call(city='Sheffield', country='United Kingdom'),
                          mock.call(city='Brussels', country='Belgium')
                          ]

        assert geocoded_dataframe.equals(expected_dataframe)
        mocked_geocode.assert_has_calls(expected_calls)

    @mock.patch('nesta.packages.health_data.seed_csv_processing.geocode')
    def test_underlying_geocoding_function_called_with_query_fallback(self, mocked_geocode, test_dataframe):
        mocked_geocode.side_effect = [None, 'cat', None, 'dog', None, 'squirrel']
        with StringIO() as temp_out:
            geocoded_dataframe = geocode_dataframe(test_dataframe, out_file=temp_out)

        expected_dataframe = pd.DataFrame({
                'index': [0, 1, 2],
                'city': ['London', 'Sheffield', 'Brussels'],
                'country': ['UK', 'United Kingdom', 'Belgium'],
                'coordinates': ['cat', 'dog', 'squirrel']
                })
        expected_calls = [mock.call(city='London', country='UK'),
                          mock.call('London+UK'),
                          mock.call(city='Sheffield', country='United Kingdom'),
                          mock.call('Sheffield+United Kingdom'),
                          mock.call(city='Brussels', country='Belgium'),
                          mock.call('Brussels+Belgium')
                          ]

        assert geocoded_dataframe.equals(expected_dataframe)
        mocked_geocode.assert_has_calls(expected_calls)

    @mock.patch('nesta.packages.health_data.seed_csv_processing.geocode')
    def test_time_between_calls_not_less_than_1_second(self, mocked_geocode, test_dataframe):
        successful_request = iter([False, False, False, True, True])

        def side_effect(*args, **kwargs):
            if next(successful_request):
                return time.time()
            return None

        mocked_geocode.side_effect = side_effect
        start_time = time.time()
        with StringIO() as temp_out:
            geocoded_dataframe = geocode_dataframe(test_dataframe, out_file=temp_out)

        assert geocoded_dataframe.coordinates[0] is None
        assert geocoded_dataframe.coordinates[1] > start_time - 3
        assert geocoded_dataframe.coordinates[2] > start_time - 4

    @mock.patch('nesta.packages.health_data.seed_csv_processing.geocode')
    def test_request_exception_doesnt_crash_process(self, mocked_geocode, test_dataframe):
        request_exceptions = iter([requests.exceptions.Timeout(),
                                   requests.exceptions.ConnectionError(),
                                   requests.exceptions.ConnectTimeout()
                                   ])

        def side_effect(*args, **kwargs):
            raise next(request_exceptions)

        mocked_geocode.side_effect = side_effect
        try:
            with StringIO() as temp_out:
                geocoded_dataframe = geocode_dataframe(test_dataframe, out_file=temp_out)
        except Exception as e:
            pytest.fail(str(e))

        expected_dataframe = pd.DataFrame({
                'index': [0, 1, 2],
                'city': ['London', 'Sheffield', 'Brussels'],
                'country': ['UK', 'United Kingdom', 'Belgium'],
                'coordinates': [None, None, None]
                })
        assert geocoded_dataframe.equals(expected_dataframe)
        assert mocked_geocode.call_count == 3

    @mock.patch('nesta.packages.health_data.seed_csv_processing.geocode')
    def test_duplicates_are_only_geocoded_once(self, mocked_geocode):
        test_dataframe = pd.DataFrame({
                'index': [0, 1, 2, 3],
                'city': ['London', 'Brussels', 'London', 'Brussels'],
                'country': ['UK', 'Belgium', 'UK', 'Belgium']
                })

        mocked_geocode.side_effect = ['LON', 'BRU']
        with StringIO() as temp_out:
            geocoded_dataframe = geocode_dataframe(test_dataframe, out_file=temp_out)

        expected_dataframe = pd.DataFrame({
                'index': [0, 1, 2, 3],
                'city': ['London', 'Brussels', 'London', 'Brussels'],
                'country': ['UK', 'Belgium', 'UK', 'Belgium'],
                'coordinates': ['LON', 'BRU', 'LON', 'BRU']
                })
        assert geocoded_dataframe.equals(expected_dataframe)
        assert mocked_geocode.call_count == 2
