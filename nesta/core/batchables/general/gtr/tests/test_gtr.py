import pytest
from unittest import mock

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


def test_reformat_row():
    pass

def test_get_project_links():
    pass

def test_get_org_locations():
    pass
