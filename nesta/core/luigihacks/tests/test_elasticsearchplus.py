import pytest
from unittest import mock
from alphabet_detector import AlphabetDetector
from collections import Counter
import time

from nesta.core.luigihacks.elasticsearchplus import Translator

from nesta.core.luigihacks.elasticsearchplus import sentence_chunks
from nesta.core.luigihacks.elasticsearchplus import _auto_translate
from nesta.core.luigihacks.elasticsearchplus import _sanitize_html
from nesta.core.luigihacks.elasticsearchplus import _clean_bad_unicode_conversion
from nesta.core.luigihacks.elasticsearchplus import _null_empty_str
from nesta.core.luigihacks.elasticsearchplus import _coordinates_as_floats
from nesta.core.luigihacks.elasticsearchplus import _country_lookup
from nesta.core.luigihacks.elasticsearchplus import _country_detection
from nesta.core.luigihacks.elasticsearchplus import COUNTRY_TAG
from nesta.core.luigihacks.elasticsearchplus import TRANS_TAG
from nesta.core.luigihacks.elasticsearchplus import LANGS_TAG
from nesta.core.luigihacks.elasticsearchplus import _guess_delimiter
from nesta.core.luigihacks.elasticsearchplus import _listify_terms
from nesta.core.luigihacks.elasticsearchplus import _null_mapping
from nesta.core.luigihacks.elasticsearchplus import _add_entity_type
from nesta.core.luigihacks.elasticsearchplus import _clean_up_lists
from nesta.core.luigihacks.elasticsearchplus import _caps_to_camel_case
from nesta.core.luigihacks.elasticsearchplus import _remove_padding
from nesta.core.luigihacks.elasticsearchplus import _nullify_pairs

from nesta.core.luigihacks.elasticsearchplus import ElasticsearchPlus

PATH="nesta.core.luigihacks.elasticsearchplus"
GUESS_DELIMITER=f"{PATH}._guess_delimiter"
SCHEMA_TRANS=f"{PATH}.schema_transformer"
CHAIN_TRANS=f"{PATH}.ElasticsearchPlus.chain_transforms"
SUPER_INDEX=f"{PATH}.Elasticsearch.index"
BOTO=f"{PATH}.boto3"
AWS4AUTH=f"{PATH}.AWS4Auth"


# This adds stability
SERVICE_URLS = [f"translate.google.{ext}"
                for ext in ('com', 'co.uk', 'co.kr', 'at',
                            'ru', 'fr', 'de', 'ch', 'es')]
TRANSLATOR = Translator(service_urls=SERVICE_URLS)


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
            "bad_unicode": "?????? something ??? else?? done?",
            "korean": "빠른 갈색 여우. 이상 점프. 게으른 개",
            "mixed_lang": "빠른 갈색 여우. Something in English.",
            "coordinate_of_xyz": {"lat": "123",
                                  "lon": "234"},
            "coordinate_of_abc": {"lat": 123,
                                  "lon": 234},
            "coordinate_of_none": None,
            "ugly_list" : ["UNKNOWN", "none", "None", None, "23", "23"],
            "empty_list" : [],
            "a negative number": -123,
            "html_field" : "<p>The yoga style is Hatha which is basic and for all levels. As I may arrange the sequences if you have some requests, please feel free to put your comments.</p> \n<p>In addition, as I love to do something for fun and make new friends, I will also create new events which is not related to yoga.</p> \n<p>Let's join us if you are interested.</p>",
            "a description field": ("Chinese and British people "
                                    "both live in Greece and Chile"),
            "terms_of_xyz": "split;me;up!;by-the-semi-colon;character;please!"
    }

@pytest.fixture
def good_doc():
    return {'_score': 100,
            'something_else': 'blah'}

@pytest.fixture
def bad_doc():
    return {'_score': 50,
            'something_else': 'blah'}

def test_sentence_chunks():
    text = '++this++++ is ++a sentence ++++++ with some +++ chunks +++'
    for i in range(0, 200):
        assert '+++'.join(sentence_chunks(text, delim='+++',
                                          chunksize=i)) == text


# def test_auto_translate_true_short(row):
#     """The translator shouldn't be applied for short pieces of text"""
#     time.sleep(2)
#     _row = _auto_translate(row, TRANSLATOR, 1000)
#     assert not _row.pop(TRANS_TAG)
#     assert len(_row.pop(LANGS_TAG)) == 0
#     assert row['korean'] == _row['korean']
#     assert row['mixed_lang'] == _row['mixed_lang']
#     assert row == _row

# def test_auto_translate_true_long_small_chunks(row):
#     time.sleep(2)
#     _row_1 = _auto_translate(row, TRANSLATOR, 10, chunksize=1)
#     time.sleep(2)
#     _row_2 = _auto_translate(row, TRANSLATOR, 10, chunksize=10000)
#     assert _row_1.pop('mixed_lang') != _row_2.pop('mixed_lang')
    
#     # Test the translation itself
#     # Constraints rather than fixed assertions since 
#     # translate algorithm may change over time
#     # so the two chunk sizes aren't guaranteed to give the same results
#     k1 = _row_1.pop('korean').upper()
#     k2 = _row_2.pop('korean').upper()
#     assert len(k1) > 10
#     assert len(k2) > 10
#     assert (len(k1) - len(k2))/len(k1) < 0.95
#     assert (len(set(k1)) - len(set(k2)))/len(set(k1)) < 0.95
#     assert sum((Counter(k1) - Counter(k2)).values())/len(k1+k2) < 0.95
#     assert sum((Counter(k2) - Counter(k1)).values())/len(k1+k2) < 0.95

#     # Confirm that the languages are the same
#     langs_1 = _row_1.pop(LANGS_TAG)
#     langs_2 = _row_2.pop(LANGS_TAG)
#     assert len(langs_1) == len(langs_2)
#     assert set(langs_1) == set(langs_2)

#     # Confirm that nothing else has changed
#     assert _row_1 == _row_2

# def test_auto_translate_true_long(row):
#     time.sleep(2)
#     _row = _auto_translate(row, TRANSLATOR, 10)
#     assert row.pop('korean') != _row['korean']
#     assert row.pop('mixed_lang') != _row['mixed_lang']
#     assert _row.pop(TRANS_TAG)
#     trans_korean = _row.pop('korean')
#     assert all(term in trans_korean.lower()
#                for term in ('brown', 'fox', 'jump',
#                             'over', 'lazy', 'dog'))
#     trans_mixed = _row.pop('mixed_lang')
#     assert all(term in trans_mixed.lower()
#                for term in ('brown', 'fox',
#                             'something', 'english'))
#     assert set(_row.pop(LANGS_TAG)) == {'ko', 'en'}
#     assert row == _row

# def test_auto_translate_false(row):
#     row.pop('korean')
#     row.pop('mixed_lang')
#     time.sleep(2)
#     _row = _auto_translate(row, TRANSLATOR)
#     assert not _row.pop(TRANS_TAG)
#     assert _row.pop(LANGS_TAG) == ['en']
#     assert row == _row

def test_sanitize_html(row):
    _row = _sanitize_html(row)
    assert len(_row) == len(row)
    for k, v in _row.items():
        if k == 'html_field':
            print(v)
            assert v == "The yoga style is Hatha which is basic and for all levels. As I may arrange the sequences if you have some requests, please feel free to put your comments. \nIn addition, as I love to do something for fun and make new friends, I will also create new events which is not related to yoga. \nLet's join us if you are interested."
        else:
            assert v == row[k]

def test_clean_bad_unicode_conversion(row):
    _row = _clean_bad_unicode_conversion(row)
    assert len(_row) == len(row)
    assert _row != row
    for k, v in _row.items():
        if type(v) is str:
            assert "??" not in v

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
    ad = AlphabetDetector()
    if type(a) is not str:
        assert a == b
    elif type(a) is str and not ad.is_latin(a):
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


def test_country_detection(row):
    _row = _country_detection(row)
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

@mock.patch(AWS4AUTH, return_value=None)
@mock.patch(BOTO)
@mock.patch(SCHEMA_TRANS, side_effect=(lambda row: row))
def test_chain_transforms(mocked_schema_transformer, mocked_boto3, mocked_auth, 
                          row, field_null_mapping):
    mocked_boto3.Session.return_value.get_credentials.return_value = mock.MagicMock()
    es = ElasticsearchPlus('dummy', aws_auth_region='blah',
                           field_null_mapping=field_null_mapping)
    _row = es.chain_transforms(row)
    assert len(_row) == len(row) + 1

@mock.patch(AWS4AUTH, return_value=None)
@mock.patch(BOTO)
@mock.patch(SUPER_INDEX, side_effect=(lambda body, **kwargs: body))
@mock.patch(CHAIN_TRANS, side_effect=(lambda row: row))
def test_index(mocked_chain_transform, mocked_super_index, 
               mocked_boto3, mocked_auth, row):
    mocked_boto3.Session.return_value.get_credentials.return_value = mock.MagicMock()
    es = ElasticsearchPlus('dummy', aws_auth_region='blah')
    with pytest.raises(ValueError):
        es.index()
    es.index(body=row)

@mock.patch(AWS4AUTH, return_value=None)
@mock.patch(BOTO)
def test_near_duplicates_no_results(mocked_boto3, mocked_auth, good_doc):
    mocked_boto3.Session.return_value.get_credentials.return_value = mock.MagicMock()
    es = ElasticsearchPlus('dummy', aws_auth_region='blah')
    es.search = mock.MagicMock(return_value={'hits':{'total':0}})
    es.get = mock.MagicMock(return_value=good_doc)
    hits = list(es.near_duplicates(index=None, doc_id=None, 
                                   doc_type=None, fields=None))
    assert hits == [good_doc]

@mock.patch(AWS4AUTH, return_value=None)
@mock.patch(BOTO)
def test_near_duplicates_some_results(mocked_boto3, mocked_auth, 
                                      good_doc, bad_doc):
    mocked_boto3.Session.return_value.get_credentials.return_value = mock.MagicMock()
    es = ElasticsearchPlus('dummy', aws_auth_region='blah')
    
    hits = [good_doc]*6 + [bad_doc]*231
    results = {'hits':{'total': len(hits), 
                       'max_score':good_doc['_score'],
                       'hits':hits}}
    es.search = mock.MagicMock(return_value=results)
    hits = list(es.near_duplicates(index=None, doc_id=None, 
                                   doc_type=None, fields=None))
    assert len(hits) == 6 # excludes bad_doc
    
