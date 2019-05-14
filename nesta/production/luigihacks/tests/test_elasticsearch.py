import pytest
from unittest import mock

from nesta.production.luigihacks.elasticsearch import _null_empty_str
from nesta.production.luigihacks.elasticsearch import _coordinates_as_floats
from nesta.production.luigihacks.elasticsearch import _country_lookup
from nesta.production.luigihacks.elasticsearch import _country_detection
from nesta.production.luigihacks.elasticsearch import COUNTRY_TAG
from nesta.production.luigihacks.elasticsearch import _guess_delimiter
from nesta.production.luigihacks.elasticsearch import _listify_terms
from nesta.production.luigihacks.elasticsearch import _null_mapping

from nesta.production.luigihacks.elasticsearch import ElasticsearchPlus

PATH="nesta.production.luigihacks.elasticsearch"
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
    return {"non-empty str": ["blah"],
            "coordinate_of_abc": [{"latitude": 123}],
            "a negative number": ["<NEGATIVE>"]}

@pytest.fixture
def row():
    return {"empty str": "",
            "non-empty str": "blah",
            "coordinate_of_xyz": {"latitude": "123",
                                  "longitude": "234"},
            "coordinate_of_abc": {"latitude": 123,
                                  "longitude": 234},
            "coordinate_of_none": None,
            "a negative number": -123,
            "a description field": ("Chinese and British people "
                                    "both live in Greece and Chile"),
            "terms_of_xyz": "split;me;up!;by-the-semi-colon;character;please!",
    }

def test_null_empty_str(row):
    _row = _null_empty_str(row)
    assert len(_row) == len(row)
    assert _row != row
    for k, v in _row.items():
        if k == "empty str":
            assert v is None
        else:
            assert v == row[k]

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

def test_null_mapping(row, field_null_mapping):
    _row = _null_mapping(row, field_null_mapping)
    assert len(_row) == len(row)
    assert _row != row
    for k, v in field_null_mapping.items():
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
    es = ElasticsearchPlus(field_null_mapping=field_null_mapping)
    _row = es.chain_transforms(row)
    assert len(_row) == len(row) + 1
    
@mock.patch(SUPER_INDEX, side_effect=(lambda body, **kwargs: body))
@mock.patch(CHAIN_TRANS, side_effect=(lambda row: row))
def test_index(mocked_chain_transform, mocked_super_index, row):
    es = ElasticsearchPlus()
    body = [row]*12
    with pytest.raises(ValueError):
        es.index()
    es.index(body=body)

