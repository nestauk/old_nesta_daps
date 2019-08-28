import pytest
from unittest import mock
from nesta.packages.cordis.cordis_api import hit_api
from nesta.packages.cordis.cordis_api import extract_fields
from nesta.packages.cordis.cordis_api import get_framework_ids
from nesta.packages.cordis.cordis_api import fetch_data

PKGPATH = 'nesta.packages.cordis.cordis_api.{}'


@mock.patch(PKGPATH.format('requests.get'))
def test_hit_api(mocked_get):
    dummy_data = 'somedata'
    mocked_get().json.return_value = {'payload': dummy_data}
    kwargs_list = [dict(api='', rcn='r', content_type='ct'),
                   dict(api='a', rcn='s', content_type='cta')]
    for kwargs in kwargs_list:
        data = hit_api(**kwargs)
        assert data == dummy_data
        (url,), _kwargs = mocked_get.call_args
        params = _kwargs['params']
        assert url.endswith(kwargs['api'])
        assert params['rcn'] == kwargs['rcn']
        assert params['contenttype'] == kwargs['content_type']
        assert len(params) == 4


def test_extract_fields():
    fields = ['fourth', 'third']
    data = {'first': '1st',
            'second': '2nd',
            'third': [{'title': '3rd', 'other': 'junk'},
                      {'title': '3ard', 'other': 'junka'}],
            'fourth': '4th'}
    out_data = extract_fields(data, fields)
    assert set(out_data.keys()) == set(fields)
    assert out_data['third'] == ['3rd', '3ard']
    assert out_data['fourth'] == data['fourth']


def test_extract_fields_bad_field():
    fields = ['fifth']
    data = {'first': '1st'}
    with pytest.raises(KeyError):
        extract_fields(data, fields)


@mock.patch(PKGPATH.format('pd'))
def test_get_framework_ids(mocked_pd):
    framework = 'h2229'
    assert type(get_framework_ids(framework)) is list
    (url,), kwargs = mocked_pd.read_csv.call_args
    assert framework in url


@mock.patch(PKGPATH.format('hit_api'))
@mock.patch(PKGPATH.format('extract_fields'))
def test_fetch_data(mocked_extract, mocked_api):
    response = fetch_data(None)
    assert type(response) is tuple
    assert len(response) == 4
