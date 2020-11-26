from unittest import mock

from nesta.core.batchables.general.nih.curate import run

PATH = "nesta.core.batchables.general.nih.curate.run.{}"
dt = run.datetime

@mock.patch(PATH.format("db_session"))
@mock.patch(PATH.format("object_to_dict"))
def test_get_projects_by_appl_id(mocked_obj2dict,
                                 mocked_db_session):
    appl_ids = ['a', 'b', 1, 2, 3]

    # Mock the session and query
    mocked_session = mock.Mock()
    q = mocked_session.query().filter().order_by().limit()
    q.all.return_value = appl_ids  # <-- will just return the input
    # Assign the session to the context manager
    mocked_db_session().__enter__.return_value = mocked_session

    # Just return the value itself
    mocked_obj2dict.side_effect = lambda obj, shallow, properties: obj

    # Test that single-member groups are created
    groups = run.get_projects_by_appl_id(None, appl_ids)
    assert groups == [[id_] for id_ in appl_ids]


def _result_factory(value):
    m = mock.Mock()
    m.base_core_project_num = value
    return m


@mock.patch(PATH.format("db_session"))
@mock.patch(PATH.format("object_to_dict"))
def test_group_projects_by_core_id(mocked_obj2dict,
                                   mocked_db_session):
    core_ids = ['a', 1, 'b', 'b', 1, 2, 1]
    results = [_result_factory(v) for v in core_ids]
    groups = [[{'base_core_project_num': 'a'}],  # Group 1
              [{'base_core_project_num': 1},     # Group 2
               {'base_core_project_num': 1},
               {'base_core_project_num': 1}],
              [{'base_core_project_num': 'b'},   # Group 3
               {'base_core_project_num': 'b'}],
              [{'base_core_project_num': 2}]]    # Group 4

    # Mock the session and query
    mocked_session = mock.Mock()
    q = mocked_session.query().filter().order_by().limit()
    q.all.return_value = results  # <-- will just return the input
    # Assign the session to the context manager
    mocked_db_session().__enter__.return_value = mocked_session

    # Just return the value itself
    mocked_obj2dict.side_effect = lambda obj, shallow, properties: obj

    # Test that single-member groups are created
    groups = run.group_projects_by_core_id(None, core_ids)
    assert groups == groups


def test_get_sim_weights():
    appl_ids = [1, 2, 3, 4]
    dupes = [{'application_id_1': 1,
              'application_id_2': 5,
              'weight': 0.4},
             {'application_id_1': 1,
              'application_id_2': 6,
              'weight': 0.9},
             {'application_id_1': 2,
              'application_id_2': 6,
              'weight': 0.8},
             {'application_id_1': 3,
              'application_id_2': 5,
              'weight': 0.3}]
    # The max weight of ids not in `appl_ids`
    sim_weights = {5: 0.4, 6: 0.9}

    assert run.get_sim_weights(dupes, appl_ids) == sim_weights

@mock.patch(PATH.format("db_session"))
@mock.patch(PATH.format("object_to_dict"))
@mock.patch(PATH.format("get_sim_weights"))
def test_retrieve_similar_projects(mocked_get_sim_weights,
                                   mocked_obj2dict,
                                   mocked_db_session):
    sim_weights = {5: 0.4, 6: 0.9}
    mocked_get_sim_weights.return_value = sim_weights
    sim_ids = [(id,) for id in set(sim_weights.keys())]
    sim_projs = [{"application_id": id} for id, in sim_ids]

    # Mock the session and query
    mocked_session = mock.MagicMock()
    q = mocked_session.query().filter()
    q.all.return_value = sim_ids  # <-- will just return the input
    # Assign the session to the context manager
    mocked_db_session().__enter__.return_value = mocked_session

    # Just return the value itself
    mocked_obj2dict.side_effect = lambda obj, shallow: obj
    assert run.retrieve_similar_projects(None, []) == (sim_projs, sim_weights)


def test_earliest_date_good_dates():
    project = {'fy': 2020,
               'project_start': '2022-1-20',
               'project_end': None,
               'award_notice_date': '2021-1-20',
               'budget_end': '2021-1-20',
               'budget_start': None}
    assert run.earliest_date(project) == dt(year=2021, month=1, day=20)


def test_earliest_date_only_year():
    project = {'fy': 2020,
               'project_start': None,
               'project_end': None,
               'award_notice_date': None,
               'budget_end': None,
               'budget_start': None}
    assert run.earliest_date(project) == dt(year=2020, month=1, day=1)


def test_earliest_date_no_dates():
    project = {'fy': None,
               'project_start': None,
               'project_end': None,
               'award_notice_date': None,
               'budget_end': None,
               'budget_start': None}
    assert run.earliest_date(project) == dt.min


@mock.patch(PATH.format('retrieve_similar_projects'))
@mock.patch(PATH.format('group_projects_by_core_id'))
@mock.patch(PATH.format('earliest_date'))
def test_retrieve_similar_proj_ids(mocked_earliest_date,
                                   mocked_group_projects,
                                   mocked_rsp):
    projs = [{'application_id': 1,
              'base_core_project_num': None},
             {'application_id': 2,
              'base_core_project_num': 'two'},
             {'application_id': 3,
              'base_core_project_num': 'three'},
             {'application_id': 4,
              'base_core_project_num': None}]
    weights = {1: 0.5, 2: 0.9, 3: 0.05, 4: 0.5,
               22: 0.1, 33: 0.7}
    groups = [[{'application_id': 22,
                'base_core_project_num': 'two'},
               {'application_id': 2,
                'base_core_project_num': 'two'}],
              [{'application_id': 33,
                'base_core_project_num': 'three'},
               {'application_id': 3,
                'base_core_project_num': 'three'}]]

    mocked_rsp.return_value = projs, weights
    mocked_group_projects.return_value = groups
    # The following will pick 22 and 33 from their groups, because
    # they are the largest value (instead of fully implementing
    # `earliest_date` in this test)
    mocked_earliest_date.side_effect = lambda x: x['application_id']
    # Note that 22 picks up the weight of 2, and 33 keeps its own
    # weight since, it is the largest weight in the group that wins
    expected = {'near_duplicate_ids': [22],
                'very_similar_ids': [33],
                'fairly_similar_ids': [1, 4]}
    assert run.retrieve_similar_proj_ids(None, None) == expected


def test_combine():
    list_of_dict = [{'a': 1}, {'a': -1}, {'a': None}]
    assert run.combine(max, list_of_dict, 'a') == 1
    assert run.combine(min, list_of_dict, 'a') == -1


def test_first_non_null():
    values = [None, None, 'foo', None, 'bar', None]
    assert run.first_non_null(values) == 'foo'


def test_join_and_dedupe():
    values = [[None, None, 'foo', None, 'bar', None],
              [None, None, 'foo', None, 'baz', None]]
    expected = ['foo', 'bar', None, 'baz']
    found = run.join_and_dedupe(values)
    assert len(expected) == len(found)
    assert set(expected) == set(found)


def test_format_us_zipcode():
    assert run.format_us_zipcode('123456789') == '12345-6789'
    assert run.format_us_zipcode('23456789')  == '02345-6789'
    assert run.format_us_zipcode('3456789')   == '00345-6789'
    assert run.format_us_zipcode('456789')    == '00045-6789'
    assert run.format_us_zipcode('56789') == '56789'
    assert run.format_us_zipcode('6789')  == '06789'
    assert run.format_us_zipcode('789')   == '00789'
    assert run.format_us_zipcode('89')    == '00089'
    assert run.format_us_zipcode('9')     == '00009'

    assert run.format_us_zipcode('anything else') == 'anything else'
    assert run.format_us_zipcode('?') == '?'


@mock.patch(PATH.format('_geocode'))
def test_geocode(mocked__geocode):
    assert run.geocode(None, None, None, None) == None

    mocked__geocode.side_effect = [None, 'bar']
    assert run.geocode(None, None, None, postalcode='something') == 'bar'

    mocked__geocode.side_effect = [None, None, 'foo']
    assert run.geocode(None, None, None, postalcode='something') == 'foo'

    mocked__geocode.side_effect = [None, 'baz']
    assert run.geocode(None, None, country='something',
                       postalcode=None) == 'baz'


def test_aggregate_group():
    proj1 = {'application_id': 1,
             'base_core_project_num': 'first',
             'fy': 2001,
             'org_city': 'Kansas City',
             'org_country': 'United States',
             'org_name': 'Big Corp',
             'org_state': None,
             'org_zipcode': '123456789',
             'project_title': 'first title',
             'ic_name': None,
             'phr': None,
             'abstract_text': 'first abstract',
             'total_cost': 100,
             # List fields
             'clinicaltrial_ids': [1,2,3],
             'clinicaltrial_titles': ['title 1', 'title 3'],
             'patent_ids': [2,3,4,5],
             'patent_titles': ['patent 1', 'patent 2'],
             'pmids': ['a', 'c', 'd'],
             'project_terms': ['AAA', 'CCC'],
             # Date fields
             'project_start': '2022-1-20',
             'project_end': None,
             'award_notice_date': '2021-1-20',
             'budget_end': '2021-1-20',
             'budget_start': None}


    proj2 = {'application_id': 2,
             'base_core_project_num': 'first',
             'fy': 2002,
             'org_city': 'Kansas City',
             'org_country': 'United States',
             'org_name': 'Big Corp',
             'org_state': None,
             'org_zipcode': '123456789',
             'project_title': 'second title',
             'ic_name': None,
             'phr': 'second phr',
             'abstract_text': 'second abstract',
             'total_cost': 200,
             # List fields
             'clinicaltrial_ids': [1,2,4],
             'clinicaltrial_titles': ['title 1', 'title 2'],
             'patent_ids': [1,3,4,5],
             'patent_titles': ['patent 1', 'patent 3'],
             'pmids': ['a', 'c', 'b'],
             'project_terms': ['AAA', 'BBB'],
             # Date fields
             'project_start': '1990-1-20',
             'project_end': None,
             'award_notice_date': '2021-1-20',
             'budget_end': '2021-1-20',
             'budget_start': None}

    proj3 = {'application_id': 2,
             'base_core_project_num': 'first',
             'fy': 2002,
             'org_city': 'Kansas City',
             'org_country': 'United States',
             'org_name': 'Big Corp',
             'org_state': 'third state',
             'org_zipcode': '123456789',
             'project_title': 'third title',
             'ic_name': 'ms third',
             'phr': None,
             'abstract_text': None,
             'total_cost': 300,
             # List fields
             'clinicaltrial_ids': [1,2,4],
             'clinicaltrial_titles': ['title 0', 'title 2'],
             'patent_ids': [1,3,4,5],
             'patent_titles': ['patent 0', 'patent 3'],
             'pmids': ['a', 'c', 'e'],
             'project_terms': ['AAA', 'DDD'],
             # Date fields
             'project_start': '1999-1-20',
             'project_end': '2025-1-20',
             'award_notice_date': '2021-1-20',
             'budget_end': '2021-1-20',
             'budget_start': None}


    group = [proj1, proj2, proj3]
    aggregated_group = {'grouped_ids': [1, 2, 2],
               'grouped_titles': ['first title', 'third title',
                                  'second title'],
               'application_id': 1,
               'base_core_project_num': 'first',
               'fy': 2001,
               'org_city': 'Kansas City',
               'org_country': 'United States',
               'org_name': 'Big Corp',
               'org_state': 'third state',
               'org_zipcode': '123456789',
               'project_title': 'first title',
               'ic_name': 'ms third',
               'phr': 'second phr',
               'abstract_text': 'first abstract',
               'clinicaltrial_ids': [1, 2, 3, 4],
               'clinicaltrial_titles': ['title 0', 'title 1',
                                        'title 2', 'title 3'],
               'patent_ids': [1, 2, 3, 4, 5],
               'patent_titles': ['patent 0', 'patent 2',
                                 'patent 3', 'patent 1'],
               'pmids': ['a', 'b', 'c', 'd', 'e'],
               'project_terms': ['AAA', 'BBB', 'CCC', 'DDD'],
               'project_start': '1990-1-20',
               'project_end': '2025-1-20',
               'total_cost': 600,
               'yearly_funds': [{'year': 1990,
                                 'project_start': '1990-1-20',
                                 'project_end': None,
                                 'total_cost': 200},
                                {'year': 1999,
                                 'project_start': '1999-1-20',
                                 'project_end': '2025-1-20',
                                 'total_cost': 300},
                                {'year': 2021,
                                 'project_start':
                                 '2022-1-20',
                                 'project_end': None,
                                 'total_cost': 100}]}

    # Check that all elements are the same
    result = run.aggregate_group(group)
    assert result.keys() == aggregated_group.keys()
    for k, v in result.items():
        _v = aggregated_group[k]
        if type(v) is list and type(v[0]) is not dict:
            assert sorted(v) == sorted(_v)
        else:
            assert v == _v
