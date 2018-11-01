import pandas as pd

from nesta.packages.health_data.process_mesh import format_mesh_terms
from nesta.packages.health_data.process_mesh import format_duplicate_map


def test_format_mesh_terms_returns_correct_format():
    test_df = pd.DataFrame({'doc_id': [2000, 2000, 2000, 3000, 3000],
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

    expected_result = {2000: ['Mesh', 'Mash'], 3000: ['Term']}
    assert format_mesh_terms(test_df) == expected_result


def test_format_duplicate_map_returns_correct_format():
    test_dupe_map = {500: 600, 888: 600, 999: 111, 998: 123, 444: 123}

    expected_result = {600: [500, 888], 111: [999], 123: [998, 444]}
    assert format_duplicate_map(test_dupe_map) == expected_result
