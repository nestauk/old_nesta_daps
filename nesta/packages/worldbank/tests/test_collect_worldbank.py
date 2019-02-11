import pytest
from unittest import mock
from json import JSONDecodeError

from nesta.packages.worldbank.collect_worldbank import DEAD_RESPONSE

from nesta.packages.worldbank.collect_worldbank import worldbank_request
from nesta.packages.worldbank.collect_worldbank import _worldbank_request
from nesta.packages.worldbank.collect_worldbank import data_from_response
from nesta.packages.worldbank.collect_worldbank import worldbank_data
from nesta.packages.worldbank.collect_worldbank import get_worldbank_resource
from nesta.packages.worldbank.collect_worldbank import get_variables_by_code
from nesta.packages.worldbank.collect_worldbank import unpack_quantity
from nesta.packages.worldbank.collect_worldbank import unpack_data
from nesta.packages.worldbank.collect_worldbank import get_country_data
from nesta.packages.worldbank.collect_worldbank import flatten_country_data
from nesta.packages.worldbank.collect_worldbank import clean_variable_names

PKG = "nesta.packages.worldbank.collect_worldbank.{}"


@pytest.fixture
def typical_worldbank_data():
    return ({'page': 1, 'pages': 304, 'per_page': '1', 'total': 304},
            [{'adminregion': {'id': '', 'iso2code': '', 'value': ''},
              'capitalCity': 'Oranjestad', 'id': 'ABW', 'name': 'Aruba'}])


@pytest.fixture
def typical_country_data():
    return {"GBR": {'Example variable name': 9.151}}


@pytest.fixture
def typical_country_metadata():
    return [{'adminregion': '',
             'capitalCity': 'London',
             'id': 'GBR',
             'incomeLevel': 'High income',
             'iso2Code': 'GB',
             'latitude': '51.5002',
             'lendingType': 'Not classified',
             'longitude': '-0.126236',
             'name': 'United Kingdom',
             'region': 'Europe & Central Asia'}]


@pytest.fixture
def typical_flat_data():
    return [{"?a bad ++ % VARiable Name!!": None}]


@pytest.fixture
def good_response():
    return ("metadata", ["data"])


@pytest.fixture
def request_kwargs():
    return dict(suffix="source", page=1, per_page=1)


# def test_worldbank_api(request_kwargs):
#     """Check the API is still up"""
#     _worldbank_request(**request_kwargs)


@mock.patch(PKG.format('requests.get'))
def test_hidden_worldbank_request_with_400(mocked_requests, request_kwargs):
    mocked_requests.return_value = mock.MagicMock()
    mocked_requests.return_value.status_code = 400
    return_value = _worldbank_request(**request_kwargs)
    assert return_value == DEAD_RESPONSE


@mock.patch(PKG.format('requests.get'))
def test_hidden_worldbank_request_with_bad_json(mocked_requests,
                                                request_kwargs):
    mocked_requests.return_value = mock.MagicMock()
    mocked_requests.return_value.json.side_effect = JSONDecodeError
    return_value = _worldbank_request(**request_kwargs)
    assert return_value == DEAD_RESPONSE


@mock.patch(PKG.format('requests.get'))
def test_hidden_worldbank_request_with_good_json(mocked_requests,
                                                 good_response,
                                                 request_kwargs):
    mocked_requests.return_value = mock.MagicMock()
    mocked_requests.return_value.json.return_value = good_response
    assert _worldbank_request(**request_kwargs) == good_response


def test_data_from_response_with_dead_response():
    assert data_from_response(DEAD_RESPONSE) == DEAD_RESPONSE


def test_data_from_response_no_key_path(good_response):
    assert data_from_response(good_response) == good_response


def test_data_from_response_dict_response_with_key_path():
    response = {"path": {"to": "value"}}
    metadata, data = data_from_response(response, ["path", "to"])
    assert metadata == response
    assert data == "value"


def test_data_from_response_mixed_response_with_key_path():
    response = {"path": [{"to": "value"}]}
    metadata, data = data_from_response(response, ["path", "to"])
    assert metadata == response
    assert data == "value"


@mock.patch(PKG.format('_worldbank_request'), return_value=DEAD_RESPONSE)
@mock.patch(PKG.format('data_from_response'), return_value=DEAD_RESPONSE)
def test_worldbank_request_with_dead_response(mocked_worldbank_request,
                                              mocked_data_from_response,
                                              request_kwargs):
    assert worldbank_request(**request_kwargs) == DEAD_RESPONSE


@mock.patch(PKG.format('worldbank_request'), return_value=DEAD_RESPONSE)
def test_worldbank_data_yielder_with_dead_response(mocked_worldbank_request):
    assert len(list(worldbank_data(""))) == 0


# Test the API is still up
def test_worldbank_data_yielder_with_good_response():
    n = len(list(worldbank_data("countries")))  # Around 300 countries expected
    assert n > 200 and n < 400


@mock.patch(PKG.format('worldbank_data'))
def test_get_worldbank_resource(mocked_worldbank_data):
    length = 100
    items = [{"key": {"value": 1}}, {"key": 1}]*length
    mocked_worldbank_data.return_value = iter(items)
    collection = get_worldbank_resource("dummy")
    assert collection == [{"key": 1}]*2*length


@mock.patch(PKG.format('get_worldbank_resource'))
@mock.patch(PKG.format('worldbank_data'))
def test_get_variables_by_code(mocked_get_worldbank_resource,
                               mocked_worldbank_data):
    dummy = [{"id": 1}, {"id": "cat"}]
    mocked_get_worldbank_resource.return_value = dummy
    mocked_worldbank_data.return_value = dummy
    variables = get_variables_by_code([2, "cat"])
    assert dict(variables) == {"cat": [1, "cat"]}


def test_unpack_quantity_good_row():
    concept = "cat"
    value = "value"
    _value = "dog"
    row = {"variable": [{"concept": concept, value: _value}]}
    assert unpack_quantity(row, concept, value) == _value


def test_unpack_quantity_bad_row():
    concept = "cat"
    value = "value"
    _value = "dog"
    row = {"variable": [{"concept": "", value: _value}]}
    with pytest.raises(NameError):
        unpack_quantity(row, concept, value)


@mock.patch(PKG.format('unpack_quantity'), return_value=None)
def test_unpack_data(mocked_unpack_quantity):
    country, variable, value = unpack_data({"value": None})
    assert mocked_unpack_quantity.call_count == 2


@mock.patch(PKG.format('worldbank_data'))
@mock.patch(PKG.format('unpack_data'))
def test_get_country_data(mocked_unpack_data,
                          mocked_worldbank_data):
    mocked_worldbank_data.return_value = iter([1, 2, 3, 4])
    mocked_unpack_data.side_effect = [("cat", "dog", "fish"),
                                      ("cat", "me", "you"),
                                      ("dog", "me", None),
                                      ("dog", "me", "you")]
    data = get_country_data({"key": ["value"]})
    assert len(data) == 2


def test_flatten_country_data(typical_country_data,
                              typical_country_metadata):
    data = flatten_country_data(typical_country_data,
                                typical_country_metadata)
    # Assert that a flat list of dictionaries is returned
    assert len(data) > 0
    assert type(data) is list
    assert type(data[0]) is dict
    assert len(data[0]) > len(typical_country_metadata[0])
    assert all(type(v) not in (list, dict) for v in data[0].values())


def test_clean_variable_names(typical_flat_data):
    cleaned_data = clean_variable_names(typical_flat_data)
    
    assert len(typical_flat_data) == len(cleaned_data)
    row = cleaned_data[0]
    assert len(row) == 1
    assert "a_bad_pc_variable_name" in row
