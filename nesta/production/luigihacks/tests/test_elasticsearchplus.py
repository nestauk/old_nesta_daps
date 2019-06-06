import pytest
from unittest import mock

from nesta.production.luigihacks.elasticsearchplus import _null_empty_str
from nesta.production.luigihacks.elasticsearchplus import _coordinates_as_floats
from nesta.production.luigihacks.elasticsearchplus import _country_lookup
from nesta.production.luigihacks.elasticsearchplus import _country_detection
from nesta.production.luigihacks.elasticsearchplus import COUNTRY_TAG
from nesta.production.luigihacks.elasticsearchplus import _guess_delimiter
from nesta.production.luigihacks.elasticsearchplus import _listify_terms
from nesta.production.luigihacks.elasticsearchplus import _null_mapping
from nesta.production.luigihacks.elasticsearchplus import _add_entity_type
from nesta.production.luigihacks.elasticsearchplus import _clean_up_lists
from nesta.production.luigihacks.elasticsearchplus import _caps_to_camel_case
from nesta.production.luigihacks.elasticsearchplus import _remove_padding
from nesta.production.luigihacks.elasticsearchplus import _nullify_pairs

from nesta.production.luigihacks.elasticsearchplus import ElasticsearchPlus

PATH="nesta.production.luigihacks.elasticsearchplus"
GUESS_DELIMITER=f"{PATH}._guess_delimiter"
SCHEMA_TRANS=f"{PATH}.schema_transformer"
CHAIN_TRANS=f"{PATH}.ElasticsearchPlus.chain_transforms"
SUPER_INDEX=f"{PATH}.Elasticsearch.index"

@pytest.fixture
def lookup():
    return {"Chinese": ["CN"],
            "British": ["GB"],
            "Greece": ["GR"],
            "France": ["FR"],
            "Chile": ["CL"]}

@pytest.fixture
def field_null_mapping():
    return {"<ALL_FIELDS>": ["None", "UNKNOWN"],
            "non-empty str": ["blah"],
            "coordinate_of_abc": [{"lat": 123}],
            "a negative number": ["<NEGATIVE>"]}

@pytest.fixture
def row():
    return {"empty str": "",
            "CAPS str": "THIS IS TOO LOUD",
            "whitespace padded": "\ntoo much padding \r",
            "whitespace padded list": ["\ntoo much padding \r"],
            "non-empty str": "blah",
            "coordinate_of_xyz": {"lat": "123",
                                  "lon": "234"},
            "coordinate_of_abc": {"lat": 123,
                                  "lon": 234},
            "coordinate_of_none": None,
            "ugly_list" : ["UNKNOWN", "none", "None", None, "23", "23"],
            "empty_list" : [],
            "a negative number": -123,
            "a description field": ("Chinese and British people "
                                    "both live in Greece and Chile"),
            "terms_of_xyz": "split;me;up!;by-the-semi-colon;character;please!"
    }

def test_nullify_pairs(row):
    _row = _nullify_pairs(row, {"coordinate_of_none": "coordinate_of_abc"})
    assert len(_row) == len(row)
    for k, v in _row.items():
        if k != "coordinate_of_abc":
            assert v == row[k]
        else:
            assert v is None
    
    
    
def test_remove_padding(row):
    _row = _remove_padding(row)
    assert len(_row) == len(row)
    assert _row != row
    assert _row["whitespace padded"] == "too much padding"
    assert _row["whitespace padded list"] == ["too much padding"]


def _test_caps_to_camel_case(a, b):
    if type(a) is not str:
        assert a == b
    elif type(a) is str and len(a) < 4 or a.upper() != a:
        assert a == b
    else:
        assert len(a) == len(b)
        assert a != b  

def test_caps_to_camel_case(row):
    _row = _caps_to_camel_case(row)
    assert len(_row) == len(row)
    assert _row != row
    for k, v in row.items():
        if type(v) is not list:
            _test_caps_to_camel_case(v, _row[k])
        else:
            for item1, item2 in zip(v,_row[k]):
                _test_caps_to_camel_case(item1, item2)


def test_null_empty_str(row):
    _row = _null_empty_str(row)
    assert len(_row) == len(row)
    assert _row != row
    for k, v in _row.items():
        if k == "empty str":
            assert v is None
        else:
            assert v == row[k]

def test_clean_up_lists(row):
    _row = _clean_up_lists(row)
    assert len(_row) == len(row)
    assert _row != row
    for k, v in row.items():
        if type(v) is not list:
            continue
        if len(v) == 0:
            assert _row[k] is None
            continue
        if None in v:
            assert None not in _row[k]
        assert list(set(_row[k])).sort() == _row[k].sort()

def test_coordinates_as_floats(row):
    _row = _coordinates_as_floats(row)
    assert len(_row) == len(row)
    assert _row != row
    for k, v in _row.items():
        if k.startswith("coordinate_") and v is not None:
            for coord_name, coord_value in v.items():
                assert type(coord_value) is float
                assert float(row[k][coord_name]) == coord_value
                assert coord_value > 0
        else:
            assert v == row[k]

def test_country_lookup():
    lookup = _country_lookup()
    all_tags = set()
    for tags in lookup.values():
        for t in tags:
            all_tags.add(t)
    assert len(lookup) > 100
    assert len(all_tags) > 100
    # Assert many-to-few mapping
    assert len(all_tags) < len(lookup)


def test_country_detection(row, lookup):
    _row = _country_detection(row, lookup)
    assert len(_row) == len(row) + 1  # added one field
    row[COUNTRY_TAG] = _row[COUNTRY_TAG]  # add that field in for the next test
    assert _row == row
    assert all(x in _row[COUNTRY_TAG]
               for x in ("CN", "GB", "CL", "GR"))
    assert type(_row[COUNTRY_TAG]) == list


def test_guess_delimiter(row):
    assert _guess_delimiter(row["terms_of_xyz"]) == ";"

@mock.patch(GUESS_DELIMITER, return_value=";")
def test_listify_splittable_terms(mocked_guess_delimiter, row):
    _row = _listify_terms(row)
    assert len(_row) == len(row)
    assert _row != row
    terms = _row["terms_of_xyz"]
    assert type(terms) is list
    assert len(terms) == 6

def test_listify_splittable_terms_with_delimiter(row):
    _row = _listify_terms(row, ";")
    assert len(_row) == len(row)
    assert _row != row
    terms = _row["terms_of_xyz"]
    assert type(terms) is list
    assert len(terms) == 6

@mock.patch(GUESS_DELIMITER, return_value=None)  # << This is a secret bonus test, since the 'return_value' of None would cause '_listify_terms' to crash, but 'terms_of_xyz' = None should mean that the code never gets that far.
def test_listify_none_terms(mocked_guess_delimiter, row):
    row["terms_of_xyz"] = None
    _row = _listify_terms(row)
    assert len(_row) == len(row)
    assert _row == row

@mock.patch(GUESS_DELIMITER, return_value=None)
def test_listify_bad_terms(mocked_guess_delimiter, row):
    row["terms_of_xyz"] = 23
    with pytest.raises(TypeError):
        _listify_terms(row)

@mock.patch(GUESS_DELIMITER, return_value=None)
def test_listify_good_terms(mocked_guess_delimiter, row):
    row["terms_of_xyz"] = row["terms_of_xyz"].split(";")
    assert _listify_terms(row) == row

def test_add_entity_type(row):
    entity_type = "something"
    _row = _add_entity_type(row, entity_type)
    assert _row.pop('type_of_entity') == entity_type
    assert _row == row

def test_null_mapping(row, field_null_mapping):
    _row = _null_mapping(row, field_null_mapping)
    print(row)
    print(_row)
    assert len(_row) == len(row)
    assert _row != row
    for k, v in field_null_mapping.items():
        if k == "<ALL_FIELDS>":
            for key, value in _row.items():
                assert value not in v
                if type(value) is list:
                    assert not any(item in v for item in value)
            continue
        assert _row[k] is None
        assert row[k] is not None
    for k, v in _row.items():
        if k in field_null_mapping:
            assert v is None
        else:
            assert v == row[k]


@mock.patch(SCHEMA_TRANS, side_effect=(lambda row: row))
def test_chain_transforms(mocked_schema_transformer, row,
                          field_null_mapping):
    es = ElasticsearchPlus('dummy', field_null_mapping=field_null_mapping)
    _row = es.chain_transforms(row)
    assert len(_row) == len(row) + 1
    
@mock.patch(SUPER_INDEX, side_effect=(lambda body, **kwargs: body))
@mock.patch(CHAIN_TRANS, side_effect=(lambda row: row))
def test_index(mocked_chain_transform, mocked_super_index, row):
    es = ElasticsearchPlus('dummy')
    with pytest.raises(ValueError):
        es.index()
    es.index(body=row)
