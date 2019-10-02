import pytest
from unittest import mock

from nesta.packages.patstat.fetch_appln_eu import pd

from nesta.packages.patstat.fetch_appln_eu import concat_dfs
from nesta.packages.patstat.fetch_appln_eu import pop_and_split
from nesta.packages.patstat.fetch_appln_eu import temp_tables_to_dfs
from nesta.packages.patstat.fetch_appln_eu import generate_temp_tables

PATH='nesta.packages.patstat.fetch_appln_eu.{}'

@pytest.fixture
def df_groups():
    data = [{'appln_id': '1,2,3', 'appln_auth':'GB,DE',
             'other':'blah'},
            {'appln_id': '4,5,6', 'appln_auth':'CN,DE',
             'other':'blah'},
            {'appln_id': '7', 'appln_auth':'CN',
             'other':'blah'}]
    return pd.DataFrame(data)

@pytest.fixture
def df_no_groups():
    data = [{'appln_id': '8', 'appln_auth':'GB',
             'other':'blah'},
            {'appln_id': '9', 'appln_auth':'DE',
             'other':'blah'},
            {'appln_id': '10', 'appln_auth':'CN',
             'other':'blah'},
            {'appln_id': '11', 'appln_auth':'CN',
             'other':'blah'}]
    return pd.DataFrame(data)


@pytest.fixture
def dfs(df_groups, df_no_groups):
    return {'groups': df_groups, 
            'no_groups': df_no_groups}

def test_concat_dfs(dfs, df_groups, df_no_groups):
    data = concat_dfs(dfs)
    assert len(data) == len(df_groups) + len(df_no_groups)
    assert all(type(row['appln_id']) is list
               for row in data)
    assert all(type(row['appln_auth']) is list
               for row in data)
    assert all('other' in row for row in data)


def test_pop_and_split():
    data = {'a': '1,2,3', 'b': 2}
    len_original = len(data)
    expected = data['a'].split(',')
    assert pop_and_split(data.copy(), 'a') == expected
    assert pop_and_split(data, 'a') == expected
    assert len(data) == len_original - 1

    expected = [str(data['b'])]
    len_original = len(data)
    assert pop_and_split(data, 'b') == expected
    assert len(data) == len_original - 1


@mock.patch(PATH.format('pd'))
def test_temp_tables_to_dfs(mocked_pd, df_groups, df_no_groups, dfs):
    mocked_pd.read_sql.side_effect = (df_groups, df_no_groups)
    assert temp_tables_to_dfs(engine=None, tables=dfs.keys()) == dfs

@mock.patch(PATH.format('Session'))
def test_generate_temp_tables(mocked_session):
    session = generate_temp_tables(engine=None)
    assert sum(arg[0][0].startswith('-- Output') 
               for arg in 
               session.execute.call_args_list) == 2
    assert len(session.execute.call_args_list) == 6
    assert len(session.commit.call_args_list) == 1
