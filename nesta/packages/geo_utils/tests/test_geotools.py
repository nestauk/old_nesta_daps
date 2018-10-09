import mock
import pytest

from nesta.packages.geo_utils.geocode import geocode

REQUESTS = 'nesta.packages.geo_utils.geocode.requests.get'

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
