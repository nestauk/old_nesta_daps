import pytest
from unittest import mock
from schema import Schema

from nesta.core.batchables.general.gtr.run import extract_funds
from nesta.core.batchables.general.gtr.run import get_linked_rows
from nesta.core.batchables.general.gtr.run import reformat_row
from nesta.core.batchables.general.gtr.run import get_project_links
from nesta.core.batchables.general.gtr.run import get_org_locations

PATH='nesta.core.batchables.general.gtr.run.{}'



@pytest.fixture
def gtr_funds():
    """There are 3 unique values here"""
    return [{'start': '1 Dec 2020', 'end': '2 Dec 2020',
             'category': 'Ingoings', 'amount': 10000, 'currencyCode': '$$$s'},
            {'start': '1 Dec 2020', 'end': '2 Dec 2020',
             'category': 'Ingoings', 'amount': 10000, 'currencyCode': '$$$s'},
            {'start': '1 Dec 2020', 'end': '2 Dec 2021',
             'category': 'Ingoings', 'amount': 10000, 'currencyCode': '$$$s'},
            {'start': '1 Dec 2020', 'end': '2 Dec 2021',
             'category': 'Ingoings', 'amount': 100, 'currencyCode': '$$$s'}]*100

@pytest.fixture
def links():
    return {'gtr_outcomes_outcomeA': [1,2,3,4],
            'gtr_aTable': [2,3,4],
            'gtr_outcomes_outcomeB': [1,3,4],
            'gtr_anotherTable': [1,4]}

@pytest.fixture
def link_table_rows():
    return [{'project_id': 1, 'table_name': 'table_1', 'id': 23},
            {'project_id': 21, 'table_name': 'table_2', 'id': 432},
            {'project_id': 1, 'table_name': 'table_1', 'id': 32},
            {'project_id': 21, 'table_name': 'table_1', 'id': 12}]

@pytest.fixture
def flattened_link_table_rows():
    return {1: {'table_1': [23, 32]},
            21: {'table_1': [12], 'table_2': [432]}}

def test_extract_funds(gtr_funds):
    output = extract_funds(gtr_funds)
    assert len(output) == 3
    assert all(len(row) == 5 for row in output)


@mock.patch(PATH.format('get_class_by_tablename'))
@mock.patch(PATH.format('object_to_dict'))
def test_get_linked_rows(mocked_obj_to_dict, mocked_get_class, links):
    session = mock.MagicMock()
    outputs = [[None]*len(ids) for ids in links.values()]
    session.query().filter().all.side_effect = outputs
    results = get_linked_rows(session, links)
    assert set(results.keys()) == set(['gtr_aTable', 'gtr_anotherTable', 'gtr_outcomes'])
    assert len(results['gtr_aTable']) == len(links['gtr_aTable'])
    assert len(results['gtr_anotherTable']) == len(links['gtr_anotherTable'])
    assert len(results['gtr_outcomes']) == len(links['gtr_outcomes_outcomeA']) + len(links['gtr_outcomes_outcomeB'])


@mock.patch(PATH.format('extract_funds'), return_value=['the funds!'])
def test_reformat_row(mocked_extract_funds):
    row = {'something': 'value'}
    linked_rows = {'gtr_funds': None,  # Mocked out
                   'gtr_topic': [{'text': 'one topic'}, {'text': 'Unclassified'}, 
                                 {'text': 'a topic'}, {'text': 'another topic'}],
                   'gtr_outcomes': 'some outcomes',
                   'gtr_organisations': [{'id': 'first org', 'name':'an org name'}]}
                   
    locations = {'first org': {'country_name': 'Japan', 'country_alpha_2': 'jp',
                               'continent': 'Asia', 'latitude': 1000, 'longitude': 200},
                 'second org': {'country_name': 'Peru', 'country_alpha_2': 'pe',
                                'continent': 'South America', 'latitude': -1000, 'longitude': -200}}
    
    row = reformat_row(row, linked_rows, locations)
    assert row == {'something': 'value',
                   'funds': ['the funds!'],
                   'outcomes': 'some outcomes',
                   'topics': ['one topic', 'a topic', 'another topic'],
                   'institutes': ['an org name'],                   
                   'institute_ids': ['first org'],
                   'countries': ['Japan'],
                   'country_alpha_2': ['jp'],
                   'continent': ['Asia'],
                   'locations': [{'lat': 1000, 'lon': 200}]}
                 

@mock.patch(PATH.format('LinkTable'))
@mock.patch(PATH.format('object_to_dict'))
def test_get_project_links(mocked_otd, mocked_LinkTable, link_table_rows, flattened_link_table_rows):
    mocked_otd.side_effect = link_table_rows
    session = mock.MagicMock()
    session.query().filter().all.return_value = [None]*len(link_table_rows)
    project_links = get_project_links(session, None)
    Schema(project_links).validate(flattened_link_table_rows)


@mock.patch(PATH.format('object_to_dict'))
def test_get_org_locations(link_table_rows):
    session = mock.MagicMock()
    session.query().all.return_value = [None]*len(link_table_rows)
    locations = get_org_locations(session)
    for row in link_table_rows:
        assert locations[row.pop('id')] == row
