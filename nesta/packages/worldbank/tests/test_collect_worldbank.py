import pytest
from unittest import mock
from json import JSONDecodeError
import types
from itertools import cycle

from nesta.packages.worldbank.collect_worldbank import DEAD_RESPONSE

from nesta.packages.worldbank.collect_worldbank import worldbank_request
from nesta.packages.worldbank.collect_worldbank import _worldbank_request
from nesta.packages.worldbank.collect_worldbank import data_from_response
from nesta.packages.worldbank.collect_worldbank import calculate_number_of_api_pages
from nesta.packages.worldbank.collect_worldbank import worldbank_data
from nesta.packages.worldbank.collect_worldbank import worldbank_data_interval
from nesta.packages.worldbank.collect_worldbank import get_worldbank_resource
from nesta.packages.worldbank.collect_worldbank import get_variables_by_code
from nesta.packages.worldbank.collect_worldbank import unpack_quantity
from nesta.packages.worldbank.collect_worldbank import unpack_data
from nesta.packages.worldbank.collect_worldbank import get_country_data
from nesta.packages.worldbank.collect_worldbank import country_data_single_request
from nesta.packages.worldbank.collect_worldbank import get_country_data_kwargs
from nesta.packages.worldbank.collect_worldbank import flatten_country_data
from nesta.packages.worldbank.collect_worldbank import clean_variable_names

PKG = "nesta.packages.worldbank.collect_worldbank.{}"


@pytest.fixture
def typical_worldbank_data():
    return ({'page': 1, 'pages': 304, 'per_page': '1', 'total': 304},
            [{'adminregion': {'id': '', 'iso2code': '', 'value': ''},
              'capitalCity': 'Oranjestad', 'id': 'ABW', 'name': 'Aruba'}])


@pytest.fixture
def typical_variables_data():
    return {"AN.EXAMPLE": [1, 34, 5],
            "ANOTHER.EXAMPLE": [13, 4]}


@pytest.fixture
def typical_aliases_data():
    # Note these match with the indexes in 'typical_variables_data'
    return {"AN.EXAMPLE": "an example (of a variable name)",
            "ANOTHER.EXAMPLE": "another example (again)"}


@pytest.fixture
def typical_country_data():
    return {"GBR": {'an example (of a variable name)':
                    [{'time': '2010',
                      'value': 51.7}],
                    'another example (again)':
                    [{'time': '2013 Q2',
                      'value': 80.4},
                     {'time': '2013 Q3',
                      'value': 80.4}]},
            "USA": {'an example (of a variable name)':
                    [{'time': '2011',
                     'value': 51.7}],
                    'another example (again)':
                    [{'time': '2012 Q1',
                      'value': 80.4}]}}


@pytest.fixture
def more_typical_country_data():
    return {"MEX": {'an example (of a variable name)':
                    [{'time': '2010',
                      'value': 51.7}],
                    'another example (again)':
                    [{'time': '2013 Q2',
                      'value': 80.4},
                     {'time': '2013 Q3',
                      'value': 80.4}]},
            "JPN": {'an example (of a variable name)':
                    [{'time': '2011',
                      'value': 51.7}],
                    'another example (again)':
                    [{'time': '2012 Q1',
                      'value': 80.4}]}}


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
             'region': 'Europe & Central Asia'},
            {'id': 'USA',
             'iso2Code': 'US',
             'name': 'United States',
             'region': 'North America',
             'adminregion': '',
             'incomeLevel': 'High income',
             'lendingType': 'Not classified',
             'capitalCity': 'Washington D.C.',
             'longitude': '-77.032',
             'latitude': '38.8895'}]


@pytest.fixture
def typical_flat_data():
    return [{"?a bad ++ % VARiable Name!!": None},
            {"barro lee_percentage_of_population_age_25_with_tertiary_schooling_completed_tertiary": None}]


@pytest.fixture
def good_response():
    return ({"total": 30}, ["data"])


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


@mock.patch(PKG.format('calculate_number_of_api_pages'))
@mock.patch(PKG.format('worldbank_request'))
def test_worldbank_data_yielder(mocked_worldbank_request,
                                mocked_page_count, good_response):
    n_pages = 13
    mocked_worldbank_request.return_value = good_response
    mocked_page_count.return_value = n_pages
    generator = worldbank_data("dummy")
    assert isinstance(generator, types.GeneratorType)
    assert len(list(generator)) == n_pages


# Test the API is still up
def test_worldbank_data_yielder_with_good_response():
    n = len(list(worldbank_data("countries")))  # Around 300 countries expected
    assert n > 200 and n < 400


@mock.patch(PKG.format('worldbank_request'))
def test_calculate_number_of_api_pages(mocked_worldbank_request,
                                       good_response):
    # Note assumes per_page is 10k
    for expected_pages, total_results in ((1, 10000),
                                          (2, 10001),
                                          (2, 20000),
                                          (4, 39999)):
        good_response[0]["total"] = total_results
        mocked_worldbank_request.return_value = good_response
        n_pages = calculate_number_of_api_pages("dummy")
        assert n_pages == expected_pages


@mock.patch(PKG.format('worldbank_request'), return_value=DEAD_RESPONSE)
def test_calculate_number_of_api_pages_dead_response(mocked_worldbank_request):
    n_pages = calculate_number_of_api_pages("dummy")
    assert n_pages == 0


@mock.patch(PKG.format('worldbank_request'))
def test_worldbank_data_interval_with_good_response(mocked_worldbank_request, good_response):
    mocked_worldbank_request.return_value = good_response
    first_page = 3
    last_page = 24
    data = list(worldbank_data_interval("dummy", first_page, last_page))
    assert len(data) == (last_page - first_page + 1)  # inclusive of last_page


@mock.patch(PKG.format('worldbank_request'), return_value=DEAD_RESPONSE)
def test_worldbank_data_interval_with_dead_response(mocked_worldbank_request):
    first_page = 3
    last_page = 24
    data = list(worldbank_data_interval("dummy", first_page, last_page))
    assert len(data) == 0


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
    country, time, value = unpack_data({"value": None})
    assert mocked_unpack_quantity.call_count == 2


@mock.patch(PKG.format('unpack_data'))
@mock.patch(PKG.format('worldbank_data_interval'))
def test_country_data_single_request(mocked_worldbank_data_interval,
                                     mocked_unpack_data):
    unpacked = [("cat_country", "dog_time", "fish_value"),
                ("cat_country", "me_time", "you_value"),
                ("dog_country", "me_time", None),
                ("dog_country", "me_time", "you_value")]
    alias = "new_key"
    a_country = "cat_country"

    mocked_worldbank_data_interval.return_value = iter([None]*len(unpacked))
    mocked_unpack_data.side_effect = unpacked
    data = country_data_single_request("new_key")
    assert len(data) == 2  # 2 countries: cat_country, dog_country
    assert len(data[a_country]) == 1  # 1 alias: new key
    # only "value" and "time" in this row of data
    _data = data[a_country][alias]
    assert len(_data) == 2
    assert "value" in _data
    assert "time" in _data


@mock.patch(PKG.format('calculate_number_of_api_pages'))
def test_get_country_data_kwargs(mocked_page_count,
                                 typical_variables_data,
                                 typical_aliases_data):
    # Purely to test the number of calls
    class CycleSummer:
        """Cumulative sum of the cycle"""
        def __init__(self, x):
            self.cycle = cycle(x)
            self._sum = 0

        def __next__(self):
            x = next(self.cycle)
            self._sum += x
            return x

        def __iter__(self):
            return self

    cycler = CycleSummer([4, 0, 3])
    mocked_page_count.side_effect = cycler
    kwargs_list = get_country_data_kwargs(typical_variables_data,
                                          typical_aliases_data)
    assert len(kwargs_list) == cycler._sum


@mock.patch(PKG.format('get_country_data_kwargs'))
@mock.patch(PKG.format('country_data_single_request'))
def test_get_country_data(mocked_single_request, mocked_get_kwargs,
                          typical_country_data, more_typical_country_data):
    n_kwargs = 3
    possible_data = [typical_country_data, more_typical_country_data]
    mocked_get_kwargs.return_value = [dict()]*n_kwargs
    mocked_single_request.side_effect = cycle(possible_data)
    result = get_country_data(variables={}, aliases={})
    expected_result = dict(**typical_country_data, **more_typical_country_data)
    assert result == expected_result


def test_flatten_country_data(typical_country_data,
                              typical_country_metadata):

    for expected_number, bad_quarters in ([2, ("Q1", "Q2", "Q3")],
                                          [3, ("Q1", "Q3", "Q4")]):
        data = flatten_country_data(typical_country_data,
                                    typical_country_metadata,
                                    bad_quarters=bad_quarters)

        # Assert that a flat list of dictionaries is returned
        assert len(data) > 0
        assert type(data) is list
        assert type(data[0]) is dict
        assert len(data[0]) > len(typical_country_metadata[0])
        assert all(type(v) not in (list, dict) for v in data[0].values())
        # Might be nice to think about how to automate the following test, rather than hard coding
        assert len(data) == expected_number


def test_clean_variable_names(typical_flat_data):
    cleaned_data = clean_variable_names(typical_flat_data)

    assert len(typical_flat_data) == len(cleaned_data)
    for row in cleaned_data:
        assert len(row) == 1
        for k in row.keys():
            assert " " not in k
            assert len(k) <= 64
            assert len(k) > 1

