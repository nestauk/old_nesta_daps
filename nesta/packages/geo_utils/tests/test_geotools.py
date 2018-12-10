import pandas as pd
import pytest
from unittest import mock

from nesta.packages.geo_utils.geocode import geocode
from nesta.packages.geo_utils.geocode import geocode_dataframe
from nesta.packages.geo_utils.geocode import geocode_batch_dataframe
from nesta.packages.geo_utils.country_iso_code import country_iso_code
from nesta.packages.geo_utils.country_iso_code import country_iso_code_dataframe
from nesta.packages.geo_utils.country_iso_code import country_iso_code_to_name

REQUESTS = 'nesta.packages.geo_utils.geocode.requests.get'
PYCOUNTRY = 'nesta.packages.geo_utils.country_iso_code.pycountry.countries.get'
_GEOCODE = 'nesta.packages.geo_utils.geocode._geocode'
COUNTRY_ISO_CODE = 'nesta.packages.geo_utils.country_iso_code.country_iso_code'


class TestGeocoding():
    @staticmethod
    @pytest.fixture
    def mocked_osm_response():
        mocked_response = mock.Mock()
        mocked_response.json.return_value = [{'lat': '12.923432', 'lon': '-75.234569'}]
        return mocked_response

    def test_error_raised_when_arguments_missing(self):
        with pytest.raises(ValueError) as e:
            geocode()
        assert "No geocode match" in str(e.value)

    @mock.patch(REQUESTS)
    def test_request_includes_user_agent_in_header(self, mocked_request, mocked_osm_response):
        mocked_request.return_value = mocked_osm_response
        geocode(something='a')
        assert mocked_request.call_args[1]['headers'] == {'User-Agent': 'Nesta health data geocode'}

    @mock.patch(REQUESTS)
    def test_url_correct_with_city_and_country(self, mocked_request, mocked_osm_response):
        mocked_request.return_value = mocked_osm_response
        kwargs = dict(city='london', country='UK')
        geocode(**kwargs)
        assert mocked_request.call_args[1]['params'] == dict(format="json", **kwargs)

    @mock.patch(REQUESTS)
    def test_url_correct_with_query(self, mocked_request, mocked_osm_response):
        mocked_request.return_value = mocked_osm_response
        kwargs = dict(q='my place')
        geocode(**kwargs)
        assert mocked_request.call_args[1]['params'] == dict(format="json", **kwargs)

    @mock.patch(REQUESTS)
    def test_error_returned_if_no_match(self, mocked_request):
        mocked_response = mock.Mock()
        mocked_response.json.return_value = []
        mocked_request.return_value = mocked_response
        with pytest.raises(ValueError) as e:
            geocode(q="Something bad")
        assert "No geocode match" in str(e.value)

    @mock.patch(REQUESTS)
    def test_coordinates_extracted_from_json_with_one_result(self, mocked_request, mocked_osm_response):
        mocked_request.return_value = mocked_osm_response
        assert geocode(q='somewhere') == [{'lat': '12.923432', 'lon': '-75.234569'}]


class TestGeocodeDataFrame():
    @staticmethod
    @pytest.fixture
    def test_dataframe():
        df = pd.DataFrame({'index': [0, 1, 2],
                           'city': ['London', 'Sheffield', 'Brussels'],
                           'country': ['UK', 'United Kingdom', 'Belgium'],
                           })
        return df

    @mock.patch(_GEOCODE)
    def test_underlying_geocoding_function_called_with_city_country(self, mocked_geocode,
                                                                    test_dataframe):
        # Generate dataframe using a mocked output
        mocked_geocode.side_effect = ['cat', 'dog', 'squirrel']
        geocoded_dataframe = geocode_dataframe(test_dataframe)

        # Expected outputs
        expected_dataframe = pd.DataFrame({'index': [0, 1, 2],
                                           'city': ['London', 'Sheffield', 'Brussels'],
                                           'country': ['UK', 'United Kingdom', 'Belgium'],
                                           'coordinates': ['cat', 'dog', 'squirrel']
                                           })
        expected_calls = [mock.call(city='London', country='UK'),
                          mock.call(city='Sheffield', country='United Kingdom'),
                          mock.call(city='Brussels', country='Belgium')]

        # Check expected behaviours
        assert geocoded_dataframe.to_dict(orient="records") == expected_dataframe.to_dict(orient="records")
        assert mocked_geocode.mock_calls == expected_calls

    @mock.patch(_GEOCODE)
    def test_underlying_geocoding_function_called_with_query_fallback(self, mocked_geocode, 
                                                                      test_dataframe):
        mocked_geocode.side_effect = [None, None, None, 'dog', 'cat', 'squirrel']
        geocoded_dataframe = geocode_dataframe(test_dataframe)
        # Expected outputs
        expected_dataframe = pd.DataFrame({'index': [0, 1, 2],
                                           'city': ['London', 'Sheffield', 'Brussels'],
                                           'country': ['UK', 'United Kingdom', 'Belgium'],
                                           'coordinates': ['dog', 'cat', 'squirrel']
                                           })
        expected_calls = [mock.call(city='London', country='UK'),
                          mock.call(city='Sheffield', country='United Kingdom'),
                          mock.call(city='Brussels', country='Belgium'),
                          mock.call('London UK'),
                          mock.call('Sheffield United Kingdom'),
                          mock.call('Brussels Belgium')]

        # Check expected behaviours
        assert geocoded_dataframe.to_dict(orient="records") == expected_dataframe.to_dict(orient="records")
        assert mocked_geocode.mock_calls == expected_calls

    @mock.patch(_GEOCODE)
    def test_duplicates_are_only_geocoded_once(self, mocked_geocode):
        test_dataframe = pd.DataFrame({'index': [0, 1, 2, 3],
                                       'city': ['London', 'Brussels', 'London', 'Brussels'],
                                       'country': ['UK', 'Belgium', 'UK', 'Belgium']
                                       })

        mocked_geocode.side_effect = ['LON', 'BRU']
        geocoded_dataframe = geocode_dataframe(test_dataframe)

        expected_dataframe = pd.DataFrame({'index': [0, 1, 2, 3],
                                           'city': ['London', 'Brussels', 'London', 'Brussels'],
                                           'country': ['UK', 'Belgium', 'UK', 'Belgium'],
                                           'coordinates': ['LON', 'BRU', 'LON', 'BRU']
                                           })

        assert geocoded_dataframe.to_dict(orient="records") == expected_dataframe.to_dict(orient="records")
        assert mocked_geocode.call_count == 2


class TestGeocodeBatchDataframe():
    @staticmethod
    @pytest.fixture
    def test_dataframe():
        df = pd.DataFrame({'index': [0, 1, 2],
                           'city': ['London', 'Sheffield', 'Brussels'],
                           'country': ['UK', 'United Kingdom', 'Belgium'],
                           })
        return df

    @mock.patch(_GEOCODE)
    def test_underlying_geocoding_function_called_with_city_country(self, mocked_geocode,
                                                                    test_dataframe):
        # Generate dataframe using a mocked output
        mocked_geocode.side_effect = [{'lat': '12.923432', 'lon': '-75.234569'},
                                      {'lat': '99.999999', 'lon': '-88.888888'},
                                      {'lat': '-2.202022', 'lon': '0.000000'}
                                      ]
        geocoded_dataframe = geocode_batch_dataframe(test_dataframe)

        # Expected outputs
        expected_dataframe = pd.DataFrame({'index': [0, 1, 2],
                                           'city': ['London', 'Sheffield', 'Brussels'],
                                           'country': ['UK', 'United Kingdom', 'Belgium'],
                                           'latitude': [12.923432, 99.999999, -2.202022],
                                           'longitude': [-75.234569, -88.888888, 0.0]
                                           })
        expected_calls = [mock.call(city='London', country='UK'),
                          mock.call(city='Sheffield', country='United Kingdom'),
                          mock.call(city='Brussels', country='Belgium')]

        # Check expected behaviours
        assert geocoded_dataframe.to_dict(orient="records") == expected_dataframe.to_dict(orient="records")
        assert mocked_geocode.mock_calls == expected_calls

    @mock.patch(_GEOCODE)
    def test_underlying_geocoding_function_called_with_query_fallback(self,
                                                                      mocked_geocode,
                                                                      test_dataframe):
        mocked_geocode.side_effect = [None,
                                      {'lat': 1, 'lon': 4},
                                      None,
                                      {'lat': 2, 'lon': 5},
                                      None,
                                      {'lat': 3, 'lon': 6}
                                      ]
        geocoded_dataframe = geocode_batch_dataframe(test_dataframe)
        # Expected outputs
        expected_dataframe = pd.DataFrame({'index': [0, 1, 2],
                                           'city': ['London', 'Sheffield', 'Brussels'],
                                           'country': ['UK', 'United Kingdom', 'Belgium'],
                                           'latitude': [1.0, 2.0, 3.0],
                                           'longitude': [4.0, 5.0, 6.0],
                                           })
        expected_calls = [mock.call(city='London', country='UK'),
                          mock.call(q='London UK'),
                          mock.call(city='Sheffield', country='United Kingdom'),
                          mock.call(q='Sheffield United Kingdom'),
                          mock.call(city='Brussels', country='Belgium'),
                          mock.call(q='Brussels Belgium')]

        # Check expected behaviours
        assert geocoded_dataframe.to_dict(orient="records") == expected_dataframe.to_dict(orient="records")
        assert mocked_geocode.mock_calls == expected_calls


class TestCountryIsoCode():
    @mock.patch(PYCOUNTRY)
    def test_lookup_via_name(self, mocked_pycountry):
        mocked_pycountry.return_value = 'country_object'
        expected_calls = [mock.call(name='United Kingdom')]

        assert country_iso_code('United Kingdom') == 'country_object'
        assert mocked_pycountry.mock_calls == expected_calls
        assert mocked_pycountry.call_count == 1

    @mock.patch(PYCOUNTRY)
    def test_lookup_via_common_name(self, mocked_pycountry):
        mocked_pycountry.side_effect = [KeyError(), 'country_object']
        expected_calls = [mock.call(name='United Kingdom'),
                          mock.call(common_name='United Kingdom')
                          ]

        assert country_iso_code('United Kingdom') == 'country_object'
        assert mocked_pycountry.mock_calls == expected_calls
        assert mocked_pycountry.call_count == 2

    @mock.patch(PYCOUNTRY)
    def test_lookup_via_official_name(self, mocked_pycountry):
        mocked_pycountry.side_effect = [KeyError(), KeyError(), 'country_object']
        expected_calls = [mock.call(name='United Kingdom'),
                          mock.call(common_name='United Kingdom'),
                          mock.call(official_name='United Kingdom')
                          ]

        assert country_iso_code('United Kingdom') == 'country_object'
        assert mocked_pycountry.mock_calls == expected_calls
        assert mocked_pycountry.call_count == 3

    @mock.patch(PYCOUNTRY)
    def test_invalid_lookup_raises_keyerror(self, mocked_pycountry):
        mocked_pycountry.side_effect = [KeyError(), KeyError(), KeyError()]

        with pytest.raises(KeyError) as e:
            country_iso_code('Fake Country')
        assert 'Fake Country not found' in str(e.value)

    @mock.patch(PYCOUNTRY)
    def test_title_case_is_applied(self, mocked_pycountry):
        expected_calls = [mock.call(name='United Kingdom'),
                          mock.call(name='United Kingdom'),
                          mock.call(name='United Kingdom')]

        country_iso_code('united kingdom')
        country_iso_code('UNITED KINGDOM')
        country_iso_code('United kingdom')
        assert mocked_pycountry.mock_calls == expected_calls


class TestCountryIsoCodeDataframe():
    @staticmethod
    def _mocked_response(alpha_2, alpha_3, numeric, continent):
        '''Builds a mocked response for the patched country_iso_code function.'''
        response = mock.Mock()
        response.alpha_2 = alpha_2
        response.alpha_3 = alpha_3
        response.numeric = numeric
        response.continent = continent
        return response

    @mock.patch(COUNTRY_ISO_CODE)
    def test_valid_countries_coded(self, mocked_country_iso_code):
        test_df = pd.DataFrame({'index': [0, 1, 2],
                                'country': ['United Kingdom', 'Belgium', 'United States']
                                })
        mocked_response_uk = self._mocked_response('GB', 'GBR', '123', 'EU')
        mocked_response_be = self._mocked_response('BE', 'BEL', '875', 'EU')
        mocked_response_us = self._mocked_response('US', 'USA', '014', 'NA')
        mocked_country_iso_code.side_effect = [mocked_response_uk,
                                               mocked_response_be,
                                               mocked_response_us
                                               ]
        expected_dataframe = pd.DataFrame(
                            {'index': [0, 1, 2],
                             'country': ['United Kingdom', 'Belgium', 'United States'],
                             'country_alpha_2': ['GB', 'BE', 'US'],
                             'country_alpha_3': ['GBR', 'BEL', 'USA'],
                             'country_numeric': ['123', '875', '014'],
                             'continent': ['EU', 'EU', 'NA']
                             })
        coded_df = country_iso_code_dataframe(test_df)
        assert coded_df.to_dict(orient="records") == expected_dataframe.to_dict(orient="records")

    @mock.patch(COUNTRY_ISO_CODE)
    def test_invalid_countries_data_is_none(self, mocked_country_iso_code):
        test_df = pd.DataFrame({'index': [0, 1, 2],
                                'country': ['United Kingdom', 'Belgium', 'United States']
                                })
        mocked_country_iso_code.side_effect = KeyError
        expected_dataframe = pd.DataFrame(
                            {'index': [0, 1, 2],
                             'country': ['United Kingdom', 'Belgium', 'United States'],
                             'country_alpha_2': [None, None, None],
                             'country_alpha_3': [None, None, None],
                             'country_numeric': [None, None, None],
                             'continent': [None, None, None]
                             })
        coded_df = country_iso_code_dataframe(test_df)
        assert coded_df.to_dict(orient="records") == expected_dataframe.to_dict(orient="records")


class TestCountryIsoCodeToName():
    def test_valid_iso_code_returns_name(self):
        assert country_iso_code_to_name('ITA') == 'Italy'
        assert country_iso_code_to_name('DEU') == 'Germany'
        assert country_iso_code_to_name('GBR') == 'United Kingdom'

    def test_invalid_iso_code_returns_none(self):
        assert country_iso_code_to_name('FOO') is None
        assert country_iso_code_to_name('ABC') is None
        assert country_iso_code_to_name('ZZZ') is None
