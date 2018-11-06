import pandas as pd
import pytest

from nesta.packages.health_data.process_mesh import format_mesh_terms
from nesta.packages.health_data.process_mesh import format_duplicate_map


@pytest.fixture
def test_df():
    return pd.DataFrame({'doc_id': [2000, 2000, 2000, 3000, 3000,
                                    '[26668]: *** ERROR *** ERROR *** ERROR ***'],
                         'term': ['PRC', 'Mesh', 'Mash', 'PRC', 'Term', pd.np.nan],
                         'term_id': ['6157405;61965;114216;418801;406325;816371;',
                                     'D000001',
                                     'D000007',
                                     '11370042;8052037;1405916;2535578;8443715;',
                                     'D999999', pd.np.nan],
                         'cui': [pd.np.nan, 'C0086418', 'C8888987',
                                 pd.np.nan, 'C1234578', pd.np.nan],
                         'score': [pd.np.nan, 222, 444, pd.np.nan, 688, pd.np.nan],
                         'indices': [pd.np.nan, '44^8^0', '812^7^0;812^7^0',
                                     pd.np.nan, pd.np.nan, pd.np.nan]
                         })


def test_format_mesh_terms_removes_prc_rows(test_df):
    formatted_terms = format_mesh_terms(test_df)
    for mesh_terms in formatted_terms.values():
        assert 'PRC' not in mesh_terms


def test_format_mesh_terms_removes_error_rows(test_df):
    formatted_terms = format_mesh_terms(test_df)
    for key in formatted_terms.keys():
        if type(key) != int:
            assert 'ERROR ***' not in key
            pytest.fail(f"keys should be integers: {key}")


def test_format_mesh_terms_doesnt_fail_when_no_error_rows():
    test_df = pd.DataFrame({'doc_id': [2000, 2000, 2000, 3000, 3000, ],
                            'term': ['PRC', 'Mesh', 'Mash', 'PRC', 'Term'],
                            'term_id': ['6157405;61965;114216;418801;406325;816371;',
                                        'D000001',
                                        'D000007',
                                        '11370042;8052037;1405916;2535578;8443715;',
                                        'D999999'],
                            'cui': [pd.np.nan, 'C0086418', 'C8888987',
                                    pd.np.nan, 'C1234578'],
                            'score': [pd.np.nan, 222, 444, pd.np.nan, 688],
                            'indices': [pd.np.nan, '44^8^0', '812^7^0;812^7^0',
                                        pd.np.nan, pd.np.nan]
                            })
    try:
        format_mesh_terms(test_df)
    except AttributeError:
        pytest.fail("File with no error rows could not be processed")


def test_format_mesh_terms_returns_correct_result(test_df):
    expected_result = {2000: ['Mesh', 'Mash'], 3000: ['Term']}
    assert format_mesh_terms(test_df) == expected_result


def test_format_duplicate_map_combines_duplicates_against_the_same_key():
    test_dupe_map = {500: 600, 888: 600, 999: 111, 998: 123, 444: 123}

    expected_result = {600: [500, 888], 111: [999], 123: [998, 444]}
    assert format_duplicate_map(test_dupe_map) == expected_result
