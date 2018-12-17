import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
import pytest

from nesta.packages.crunchbase.crunchbase_collect import rename_uuid_columns
from nesta.packages.crunchbase.crunchbase_collect import process_orgs
from nesta.packages.crunchbase.crunchbase_collect import split_batches
from nesta.packages.crunchbase.crunchbase_collect import total_records
from nesta.packages.crunchbase.crunchbase_collect import bool_convert


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


def test_bool_convert():
    assert bool_convert('t') is True
    assert bool_convert('f') is False
    assert bool_convert('aaa') is None
    assert bool_convert(None) is None


@pytest.fixture
def generate_test_data():
    def _generate_test_data(n):
        return [{'data': 'foo', 'other': 'bar'} for i in range(n)]
    return _generate_test_data


def test_split_batches_when_data_is_smaller_than_batch_size(generate_test_data):
    yielded_batches = []
    for batch in split_batches(generate_test_data(200), batch_size=1000):
        yielded_batches.append(batch)

    assert len(yielded_batches) == 1


def test_split_batches_yields_multiple_batches_with_exact_fit(generate_test_data):
    yielded_batches = []
    for batch in split_batches(generate_test_data(2000), batch_size=1000):
        yielded_batches.append(batch)

    assert len(yielded_batches) == 2


def test_split_batches_yields_multiple_batches_with_remainder(generate_test_data):
    yielded_batches = []
    for batch in split_batches(generate_test_data(2400), batch_size=1000):
        yielded_batches.append(batch)

    assert len(yielded_batches) == 3


def test_total_records_returns_correct_totals(generate_test_data):
    returned_data = {'inserted': generate_test_data(100),
                     'existing': generate_test_data(0),
                     'failed': generate_test_data(230)
                     }
    expected_result = {'inserted': 100,
                       'existing': 0,
                       'failed': 230,
                       'total': 330,
                       'batch_total': 330
                       }
    assert total_records(returned_data) == expected_result


def test_total_records_returns_correct_totals_with_batches(generate_test_data):
    returned_data = {'inserted': generate_test_data(0),
                     'existing': generate_test_data(40),
                     'failed': generate_test_data(920)
                     }
    previous_totals = {'inserted': 100,
                       'existing': 0,
                       'failed': 230,
                       'total': 330,
                       'batch_total': 100
                       }
    expected_result = {'inserted': 100,
                       'existing': 40,
                       'failed': 1150,
                       'total': 1290,
                       'batch_total': 960
                       }
    assert total_records(returned_data, previous_totals) == expected_result


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
def existing_orgs():
    return {'2-2', '3-3'}


@pytest.fixture
def no_existing_orgs():
    return set()


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


def test_process_orgs_renames_uuid_column(valid_org_data, no_existing_orgs,
                                          valid_cat_groups, valid_org_descs):
    processed_orgs, _, _ = process_orgs(valid_org_data, no_existing_orgs, valid_cat_groups, valid_org_descs)
    processed_orgs = pd.DataFrame(processed_orgs)

    assert 'id' in processed_orgs
    assert 'uuid' not in processed_orgs


def test_process_orgs_correctly_applies_country_name(valid_org_data, no_existing_orgs,
                                                     valid_cat_groups, valid_org_descs):
    processed_orgs, _, _ = process_orgs(valid_org_data, no_existing_orgs,
                                        valid_cat_groups, valid_org_descs)
    processed_orgs = pd.DataFrame(processed_orgs)
    expected_result = pd.Series(['France', 'Germany', 'United Kingdom'])

    assert_series_equal(processed_orgs['country'], expected_result, check_names=False)


def test_process_orgs_removes_country_code_column(valid_org_data, no_existing_orgs,
                                                  valid_cat_groups, valid_org_descs):
    processed_orgs, _, _ = process_orgs(valid_org_data, no_existing_orgs,
                                        valid_cat_groups, valid_org_descs)
    processed_orgs = pd.DataFrame(processed_orgs)

    assert 'country_code' not in processed_orgs


def test_process_orgs_generates_location_id_composite_keys(valid_org_data, no_existing_orgs,
                                                           valid_cat_groups, valid_org_descs):
    processed_orgs, _, _ = process_orgs(valid_org_data, no_existing_orgs,
                                        valid_cat_groups, valid_org_descs)
    processed_orgs = pd.DataFrame(processed_orgs)
    expected_result = pd.Series(['paris_france', 'berlin_germany', 'london_united-kingdom'])

    assert_series_equal(processed_orgs.location_id, expected_result, check_names=False)


def test_process_orgs_inserts_none_if_composite_key_fails(invalid_org_data, no_existing_orgs,
                                                          valid_cat_groups, valid_org_descs):
    processed_orgs, _, _ = process_orgs(invalid_org_data, no_existing_orgs,
                                        valid_cat_groups, valid_org_descs)
    processed_orgs = pd.DataFrame(processed_orgs)
    expected_result = pd.Series([None, 'berlin_germany', 'london_united-kingdom'])

    assert_series_equal(processed_orgs.location_id, expected_result, check_names=False)


def test_process_orgs_generates_org_cats_link_table(valid_org_data, no_existing_orgs,
                                                    valid_cat_groups, valid_org_descs):
    _, org_cats, _ = process_orgs(valid_org_data, no_existing_orgs,
                                  valid_cat_groups, valid_org_descs)
    expected_result = [{'organization_id': '1-1', 'category_name': 'data'},
                       {'organization_id': '1-1', 'category_name': 'digital'},
                       {'organization_id': '1-1', 'category_name': 'cats'},
                       {'organization_id': '2-2', 'category_name': 'science'},
                       {'organization_id': '2-2', 'category_name': 'cats'},
                       {'organization_id': '3-3', 'category_name': 'data'}
                       ]

    assert org_cats == expected_result


def test_process_orgs_returns_missing_cat_groups(invalid_org_data, no_existing_orgs,
                                                 valid_cat_groups, valid_org_descs):
    _, org_cats, missing_cat_groups = process_orgs(invalid_org_data, no_existing_orgs,
                                                   valid_cat_groups, valid_org_descs)
    expected_org_cats = [{'organization_id': '1-1', 'category_name': 'data'},
                         {'organization_id': '1-1', 'category_name': 'digital'},
                         {'organization_id': '1-1', 'category_name': 'dogs'},
                         {'organization_id': '2-2', 'category_name': 'science'},
                         {'organization_id': '2-2', 'category_name': 'cats'},
                         {'organization_id': '2-2', 'category_name': 'goats'}
                         ]
    missing_cats = {c['category_name'] for c in missing_cat_groups}
    expected_missing_cat_groups = {'dogs', 'goats'}

    assert org_cats == expected_org_cats
    assert missing_cats == expected_missing_cat_groups


def test_process_orgs_appends_long_descriptions(valid_org_data, no_existing_orgs,
                                                valid_cat_groups, valid_org_descs):
    processed_orgs, _, _ = process_orgs(valid_org_data, no_existing_orgs,
                                        valid_cat_groups, valid_org_descs)
    processed_orgs = pd.DataFrame(processed_orgs)
    expected_result = pd.DataFrame({'id': ['1-1', '2-2', '3-3'],
                                    'long_description': ['org one', 'org two', 'org three']})

    assert_frame_equal(processed_orgs[['id', 'long_description']], expected_result, check_like=True)


def test_process_orgs_inserts_none_for_unfound_long_descriptions(valid_org_data, no_existing_orgs,
                                                                 valid_cat_groups, invalid_org_descs):
    processed_orgs, _, _ = process_orgs(valid_org_data, no_existing_orgs,
                                        valid_cat_groups, invalid_org_descs)
    processed_orgs = pd.DataFrame(processed_orgs)
    expected_result = pd.DataFrame({'id': ['1-1', '2-2', '3-3'],
                                    'long_description': [None, 'org two', 'org three']})

    assert_frame_equal(processed_orgs[['id', 'long_description']], expected_result, check_like=True)


def test_process_orgs_removes_redundant_category_columns(valid_org_data, no_existing_orgs,
                                                         valid_cat_groups, valid_org_descs):
    processed_orgs, _, _ = process_orgs(valid_org_data, no_existing_orgs,
                                        valid_cat_groups, valid_org_descs)
    processed_orgs = pd.DataFrame(processed_orgs)

    assert 'category_list' not in processed_orgs
    assert 'category_group_list' not in processed_orgs


def test_process_orgs_removes_existing_orgs(valid_org_data, existing_orgs,
                                            valid_cat_groups, valid_org_descs):
    processed_orgs, _, _ = process_orgs(valid_org_data, existing_orgs,
                                        valid_cat_groups, valid_org_descs)
    processed_orgs = pd.DataFrame(processed_orgs)

    expected_result = pd.Series(['1-1'])
    assert_series_equal(processed_orgs['id'], expected_result, check_names=False)
