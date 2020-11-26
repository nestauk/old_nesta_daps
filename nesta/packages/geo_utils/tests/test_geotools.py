import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
from unittest import mock

from nesta.packages.geo_utils.geocode import geocode
from nesta.packages.geo_utils.geocode import _geocode
from nesta.packages.geo_utils.geocode import geocode_dataframe
from nesta.packages.geo_utils.geocode import geocode_batch_dataframe
from nesta.packages.geo_utils.geocode import generate_composite_key
from nesta.packages.geo_utils.country_iso_code import country_iso_code
from nesta.packages.geo_utils.country_iso_code import country_iso_code_dataframe
from nesta.packages.geo_utils.country_iso_code import country_iso_code_to_name
from nesta.packages.geo_utils.lookup import get_continent_lookup
from nesta.packages.geo_utils.lookup import get_country_region_lookup
from nesta.packages.geo_utils.lookup import get_country_continent_lookup

REQUESTS = 'nesta.packages.geo_utils.geocode.requests.get'
PYCOUNTRY = 'nesta.packages.geo_utils.country_iso_code.pycountry.countries.get'
GEOCODE = 'nesta.packages.geo_utils.geocode.geocode'
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

    @mock.patch(GEOCODE)
    def test_geocode_wrapper_rejects_invalid_query_parameters(self, mocked_geocode):
        with pytest.raises(ValueError) as e:
            _geocode(cat='dog', city='Nice')
        assert "Invalid query parameter" in str(e.value)

    @mock.patch(GEOCODE)
    def test_geocode_wrapper_rejects_both_q_and_kwargs_supplied(self, mocked_geocode):
        with pytest.raises(ValueError) as e:
            _geocode(city='London', q='somewhere')
        assert "Supply either q OR other query parameters, they cannot be combined." in str(e.value)

    @mock.patch(GEOCODE)
    def test_geocode_wrapper_errors_if_no_query_parameters_supplied(self, mocked_geocode):
        with pytest.raises(ValueError) as e:
            _geocode()
        assert "No query parameters supplied" in str(e.value)

    @mock.patch(GEOCODE)
    def test_geocode_wrapper_calls_geocode_properly(self, mocked_geocode):
        mocked_geocode.return_value = [{'lat': 1.1, 'lon': 2.2}]

        _geocode('my place')
        _geocode(q='somewhere')
        _geocode(city='London', country='UK')
        _geocode(postalcode='ABC 123')

        expected_calls = [mock.call(q='my place'),
                          mock.call(q='somewhere'),
                          mock.call(city='London', country='UK'),
                          mock.call(postalcode='ABC 123')
                          ]
        assert mocked_geocode.mock_calls == expected_calls


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

        geocoded_dataframe = geocode_batch_dataframe(test_dataframe)

        # Check expected behaviours
        assert_frame_equal(geocoded_dataframe, expected_dataframe,
                           check_like=True, check_dtype=False)
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

        geocoded_dataframe = geocode_batch_dataframe(test_dataframe, query_method='both')

        # Check expected behaviours
        assert_frame_equal(geocoded_dataframe, expected_dataframe,
                           check_like=True, check_dtype=False)
        assert mocked_geocode.mock_calls == expected_calls

    @mock.patch(_GEOCODE)
    def test_underlying_geocoding_function_called_with_query_method_only(self,
                                                                         mocked_geocode,
                                                                         test_dataframe):
        mocked_geocode.side_effect = [{'lat': 1, 'lon': 4},
                                      {'lat': 2, 'lon': 5},
                                      {'lat': 3, 'lon': 6}
                                      ]
        # Expected outputs
        expected_dataframe = pd.DataFrame({'index': [0, 1, 2],
                                           'city': ['London', 'Sheffield', 'Brussels'],
                                           'country': ['UK', 'United Kingdom', 'Belgium'],
                                           'latitude': [1.0, 2.0, 3.0],
                                           'longitude': [4.0, 5.0, 6.0],
                                           })
        expected_calls = [mock.call(q='London UK'),
                          mock.call(q='Sheffield United Kingdom'),
                          mock.call(q='Brussels Belgium')]

        geocoded_dataframe = geocode_batch_dataframe(test_dataframe, query_method='query_only')

        # Check expected behaviours
        assert_frame_equal(geocoded_dataframe, expected_dataframe,
                           check_like=True, check_dtype=False)
        assert mocked_geocode.mock_calls == expected_calls

    @mock.patch(_GEOCODE)
    def test_valueerror_raised_when_invalid_query_method_passed(self,
                                                                mocked_geocode,
                                                                test_dataframe):
        with pytest.raises(ValueError):
            geocode_batch_dataframe(test_dataframe, query_method='cats')

        with pytest.raises(ValueError):
            geocode_batch_dataframe(test_dataframe, query_method='test')

        with pytest.raises(ValueError):
            geocode_batch_dataframe(test_dataframe, query_method=1)

    @mock.patch(_GEOCODE)
    def test_output_column_names_are_applied(self, mocked_geocode, test_dataframe):

        # Generate dataframe using a mocked output
        mocked_geocode.side_effect = [{'lat': '12.923432', 'lon': '-75.234569'},
                                      {'lat': '99.999999', 'lon': '-88.888888'},
                                      {'lat': '-2.202022', 'lon': '0.000000'}
                                      ]

        # Expected outputs
        expected_dataframe = pd.DataFrame({'index': [0, 1, 2],
                                           'city': ['London', 'Sheffield', 'Brussels'],
                                           'country': ['UK', 'United Kingdom', 'Belgium'],
                                           'lat': [12.923432, 99.999999, -2.202022],
                                           'lon': [-75.234569, -88.888888, 0.0]
                                           })

        geocoded_dataframe = geocode_batch_dataframe(test_dataframe,
                                                     latitude='lat',
                                                     longitude='lon')

        # Check expected behaviours
        assert_frame_equal(geocoded_dataframe, expected_dataframe,
                           check_like=True, check_dtype=False)


class TestCountryIsoCode():
    @mock.patch(PYCOUNTRY)
    def test_lookup_via_name(self, mocked_pycountry):
        mocked_pycountry.return_value = 'country_object'
        expected_calls = [mock.call(name='United Kingdom')]

        assert country_iso_code('United Kingdom') == 'country_object'
        assert mocked_pycountry.mock_calls == expected_calls
        assert mocked_pycountry.call_count == 1
        country_iso_code.cache_clear()

    @mock.patch(PYCOUNTRY)
    def test_lookup_via_common_name(self, mocked_pycountry):
        mocked_pycountry.side_effect = [KeyError(), 'country_object']
        expected_calls = [mock.call(name='United Kingdom'),
                          mock.call(common_name='United Kingdom')
                          ]

        assert country_iso_code('United Kingdom') == 'country_object'
        assert mocked_pycountry.mock_calls == expected_calls
        assert mocked_pycountry.call_count == 2
        country_iso_code.cache_clear()

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
        country_iso_code.cache_clear()

    @mock.patch(PYCOUNTRY)
    def test_invalid_lookup_raises_keyerror(self, mocked_pycountry):
        mocked_pycountry.side_effect = [KeyError(), KeyError(), KeyError()]*2

        with pytest.raises(KeyError) as e:
            country_iso_code('Fake Country')
        assert 'Fake Country not found' in str(e.value)
        country_iso_code.cache_clear()

    @mock.patch(PYCOUNTRY)
    def test_title_case_is_applied(self, mocked_pycountry):
        expected_calls = []
        names = ['united kingdom', 'UNITED KINGDOM',
                 'United kingdom']
        mocked_pycountry.side_effect = [KeyError(), KeyError(), KeyError(), 'blah'] * len(names)
        for name in names:
            country_iso_code(name)  # Find the iso codes
            raw_call = mock.call(name=name)
            common_call = mock.call(common_name=name)
            official_call = mock.call(official_name=name)
            title_call = mock.call(name='United Kingdom')
            expected_calls.append(raw_call)  # The initial call
            expected_calls.append(common_call)  # Tries common name call
            expected_calls.append(official_call)  # Tries official name
            expected_calls.append(title_call) # The title case call
        assert mocked_pycountry.mock_calls == expected_calls
    country_iso_code.cache_clear()


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


def test_generate_composite_key():
    assert generate_composite_key('London', 'United Kingdom') == 'london_united-kingdom'
    assert generate_composite_key('Paris', 'France') == 'paris_france'
    assert generate_composite_key('Name-with hyphen', 'COUNTRY') == 'name-with-hyphen_country'


def test_generate_composite_key_raises_error_with_invalid_input():
    with pytest.raises(ValueError):
        generate_composite_key(None, 'UK')

    with pytest.raises(ValueError):
        generate_composite_key('city_only')

    with pytest.raises(ValueError):
        generate_composite_key(1, 2)


def test_get_continent_lookup():
    continents = get_continent_lookup()
    assert None in continents
    assert '' in continents
    assert continents['NA'] == 'North America'
    assert len(continents) == 9  # 2 nulls + 7 continents

def test_get_country_region_lookup():
    countries = get_country_region_lookup()
    assert len(countries) > 100
    assert len(countries) < 1000
    assert all(len(k) == 2 for k in countries.keys())
    assert all(type(v) is tuple for v in countries.values())
    assert all(len(v) == 2 for v in countries.values())
    all_regions = {v[1] for v in countries.values()}
    assert len(all_regions) == 18


def test_country_continent_lookup():
    lookup = get_country_continent_lookup()
    non_nulls = {k: v for k, v in lookup.items()
                 if k is not None and k != ''}
    # All iso2, so length == 2
    assert all(len(k) == 2 for k in non_nulls.items())
    assert all(len(v) == 2 for v in non_nulls.values())
    # Either strings or Nones
    country_types = set(type(v) for v in lookup.values())
    assert country_types == {str, type(None)}
    # Right ball-park of country and continent numbers
    assert len(non_nulls) > 100  # num countries
    assert len(non_nulls) < 1000 # num countries
    assert len(set(non_nulls.values())) == 7 # num continents
