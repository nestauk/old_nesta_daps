import pytest
from unittest import mock

from nesta.packages.geo_utils.geocode import geocode
from nesta.packages.geo_utils.country_iso_code import country_iso_code

REQUESTS = 'nesta.packages.geo_utils.geocode.requests.get'
PYCOUNTRY = 'nesta.packages.geo_utils.country_iso_code.pycountry.countries.get'


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
