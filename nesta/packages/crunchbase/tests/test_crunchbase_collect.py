import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
import pytest

from nesta.packages.crunchbase.crunchbase_collect import rename_uuid_columns
from nesta.packages.crunchbase.crunchbase_collect import generate_composite_key
from nesta.packages.crunchbase.crunchbase_collect import process_orgs


def test_rename_uuid_columns():
    test_df = pd.DataFrame({'uuid': [1, 2, 3],
                            'org_uuid': [11, 22, 33],
                            'other_id': [111, 222, 333]
                            })

    expected_result = pd.DataFrame({'id': [1, 2, 3],
                                    'other_id': [111, 222, 333],
                                    'org_id': [11, 22, 33]
                                    })

    assert_frame_equal(rename_uuid_columns(test_df), expected_result, check_like=True)


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


@pytest.fixture
def valid_org_data():
    return pd.DataFrame({'uuid': ['1-1', '2-2', '3-3'],
                         'country_code': ['FRA', 'DEU', 'GBR'],
                         'category_list': ['Data,Digital,Cats', 'Science,Cats', 'Data'],
                         'category_group_list': ['Groups', 'More groups', 'extra group'],
                         'city': ['Paris', 'Berlin', 'London']
                         })


@pytest.fixture
def invalid_org_data():
    return pd.DataFrame({'uuid': ['1-1', '2-2', '3-3'],
                         'country_code': ['FRI', 'DEU', 'GBR'],
                         'category_list': ['Data,Digital,Dogs', 'Science,Cats,Goats', pd.np.nan],
                         'category_group_list': ['Groups', 'More groups', 'extra group'],
                         'city': [None, 'Berlin', 'London']
                         })


@pytest.fixture
def valid_cat_groups():
    return pd.DataFrame({'id': ['A', 'B', 'C', 'D'],
                         'category_name': ['data', 'digital', 'cats', 'science'],
                         'category_group_list': ['Group', 'Groups', 'Grep', 'Grow']
                         })


@pytest.fixture
def valid_org_descs():
    return pd.DataFrame({'uuid': ['3-3', '2-2', '1-1'],
                         'description': ['org three', 'org two', 'org one']
                         })


@pytest.fixture
def invalid_org_descs():
    return pd.DataFrame({'uuid': ['3-3', '2-2'],
                         'description': ['org three', 'org two']
                         })


def test_process_orgs_renames_uuid_column(valid_org_data, valid_cat_groups, valid_org_descs):
    processed_orgs, _, _ = process_orgs(valid_org_data, valid_cat_groups, valid_org_descs)
    processed_orgs = pd.DataFrame(processed_orgs)

    assert 'id' in processed_orgs
    assert 'uuid' not in processed_orgs


def test_process_orgs_correctly_applies_country_name(valid_org_data, valid_cat_groups, valid_org_descs):
    processed_orgs, _, _ = process_orgs(valid_org_data, valid_cat_groups, valid_org_descs)
    processed_orgs = pd.DataFrame(processed_orgs)
    expected_result = pd.Series(['France', 'Germany', 'United Kingdom'])

    assert_series_equal(processed_orgs['country'], expected_result, check_names=False)


def test_process_orgs_removes_country_code_column(valid_org_data, valid_cat_groups, valid_org_descs):
    processed_orgs, _, _ = process_orgs(valid_org_data, valid_cat_groups, valid_org_descs)
    processed_orgs = pd.DataFrame(processed_orgs)

    assert 'country_code' not in processed_orgs


def test_process_orgs_generates_location_id_composite_keys(valid_org_data, valid_cat_groups, valid_org_descs):
    processed_orgs, _, _ = process_orgs(valid_org_data, valid_cat_groups, valid_org_descs)
    processed_orgs = pd.DataFrame(processed_orgs)
    expected_result = pd.Series(['paris_france', 'berlin_germany', 'london_united-kingdom'])

    assert_series_equal(processed_orgs.location_id, expected_result, check_names=False)


def test_process_orgs_inserts_none_if_composite_key_fails(invalid_org_data, valid_cat_groups, valid_org_descs):
    processed_orgs, _, _ = process_orgs(invalid_org_data, valid_cat_groups, valid_org_descs)
    processed_orgs = pd.DataFrame(processed_orgs)
    expected_result = pd.Series([None, 'berlin_germany', 'london_united-kingdom'])

    assert_series_equal(processed_orgs.location_id, expected_result, check_names=False)


def test_process_orgs_generates_org_cats_link_table(valid_org_data, valid_cat_groups, valid_org_descs):
    _, org_cats, _ = process_orgs(valid_org_data, valid_cat_groups, valid_org_descs)
    expected_result = [{'organization_id': '1-1', 'category_name': 'data'},
                       {'organization_id': '1-1', 'category_name': 'digital'},
                       {'organization_id': '1-1', 'category_name': 'cats'},
                       {'organization_id': '2-2', 'category_name': 'science'},
                       {'organization_id': '2-2', 'category_name': 'cats'},
                       {'organization_id': '3-3', 'category_name': 'data'}
                       ]
    assert org_cats == expected_result


def test_process_orgs_returns_missing_cat_groups(invalid_org_data, valid_cat_groups, valid_org_descs):
    _, org_cats, missing_cat_groups = process_orgs(invalid_org_data, valid_cat_groups, valid_org_descs)
    expected_org_cats = [{'organization_id': '1-1', 'category_name': 'data'},
                         {'organization_id': '1-1', 'category_name': 'digital'},
                         {'organization_id': '1-1', 'category_name': 'dogs'},
                         {'organization_id': '2-2', 'category_name': 'science'},
                         {'organization_id': '2-2', 'category_name': 'cats'},
                         {'organization_id': '2-2', 'category_name': 'goats'}
                         ]
    expected_missing_cat_groups = [{'category_name': 'dogs'},
                                   {'category_name': 'goats'}
                                   ]

    assert org_cats == expected_org_cats
    assert missing_cat_groups == expected_missing_cat_groups


def test_process_orgs_appends_long_descriptions(valid_org_data, valid_cat_groups, valid_org_descs):
    processed_orgs, _, _ = process_orgs(valid_org_data, valid_cat_groups, valid_org_descs)
    processed_orgs = pd.DataFrame(processed_orgs)
    expected_result = pd.DataFrame({'id': ['1-1', '2-2', '3-3'],
                                    'long_description': ['org one', 'org two', 'org three']})

    assert_frame_equal(processed_orgs[['id', 'long_description']], expected_result, check_like=True)


def test_process_orgs_inserts_none_for_unfound_long_descriptions(valid_org_data, valid_cat_groups, invalid_org_descs):
    processed_orgs, _, _ = process_orgs(valid_org_data, valid_cat_groups, invalid_org_descs)
    processed_orgs = pd.DataFrame(processed_orgs)
    expected_result = pd.DataFrame({'id': ['1-1', '2-2', '3-3'],
                                    'long_description': [None, 'org two', 'org three']})

    assert_frame_equal(processed_orgs[['id', 'long_description']], expected_result, check_like=True)


def test_process_orgs_removes_redundant_category_columns(valid_org_data, valid_cat_groups, valid_org_descs):
    processed_orgs, _, _ = process_orgs(valid_org_data, valid_cat_groups, valid_org_descs)
    processed_orgs = pd.DataFrame(processed_orgs)

    assert 'category_list' not in processed_orgs
    assert 'category_group_list' not in processed_orgs
