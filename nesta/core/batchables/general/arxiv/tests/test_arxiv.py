import pytest
from unittest import mock

from nesta.core.batchables.general.arxiv.run import generate_grid_lookup
from nesta.core.batchables.general.arxiv.run import flatten_fos
from nesta.core.batchables.general.arxiv.run import flatten_categories
from nesta.core.batchables.general.arxiv.run import calculate_nuts_regions
from nesta.core.batchables.general.arxiv.run import generate_authors_and_institutes

# Just an import, not testing this
from nesta.core.batchables.general.arxiv.run import NutsFinder

PATH='nesta.core.batchables.general.arxiv.run.{}'

class Object(object):
    pass

@pytest.fixture
def institutes():
    insts = [{'country_code': 'FR', 'name': 'universite de vie',
              'latitude': 48.8566, 'longitude': 2.3522, 'id': 1},
             {'country_code': 'IT', 'name': 'universita de vita',
              'latitude': 45.4064, 'longitude': 11.8768, 'id': 2},
             {'country_code': 'GB', 'name': 'university of life',
              'latitude': None, 'longitude': None, 'id': 3}]
    _insts = []
    for inst in insts:
        _inst = Object()
        for k, v in inst.items():
            setattr(_inst, k, v)
        _insts.append(_inst)
    return _insts


@pytest.fixture
def grid_lookup():
    from collections import defaultdict
    _grid_lookup = defaultdict(Object)
    _grid_lookup[1].country = 'FR'
    _grid_lookup[1].name = 'universite de vie'
    _grid_lookup[1].region = ('France', 'Western Europe')
    _grid_lookup[1].latlon = (48.8566, 2.3522)
    _grid_lookup[2].country = 'IT'
    _grid_lookup[2].name = 'universita de vita'
    _grid_lookup[2].region = ('Italy', 'Southern Europe')
    _grid_lookup[2].latlon = (45.4064, 11.8768)
    _grid_lookup[3].country = 'GB'
    _grid_lookup[3].name = 'university of life'
    _grid_lookup[3].region = ('United Kingdom of Great Britain and Northern Ireland', 'Northern Europe')
    _grid_lookup[3].latlon = (None, None)
    return _grid_lookup

@mock.patch(PATH.format('db_session'))
def test_generate_grid_lookup(mocked_db_session, institutes, grid_lookup):
    
    mocked_session = mock.Mock()
    mocked_session.query().all.return_value = institutes
    mocked_db_session().__enter__.return_value = mocked_session    
    _grid_lookup = generate_grid_lookup(engine=None)
    for k, v in _grid_lookup.items():
        assert grid_lookup[k].__dict__ == v.__dict__

def test_flatten_fos():
    row = {'fields_of_study': {'nodes': [['physics', 'maths'], [], ['geography']]}}
    assert flatten_fos(row) == ['physics', 'maths', 'geography']


def test_flatten_categories():
    categories = [{'description': 'something'}, {'description': 'something else'}]
    assert flatten_categories(categories) == ['something', 'something else']


def test_calculate_nuts_regions(grid_lookup):
    nuts_finder = NutsFinder()
    row = {'a field': 'a value', 'another field': 'another value'}
    calculate_nuts_regions(row, grid_lookup.values(), nuts_finder)
    assert row == {'a field': 'a value', 'another field': 'another value',
                   'nuts_0': ['FR', 'IT'],
                   'nuts_1': ['FR1', 'ITH'],
                   'nuts_2': ['FR10', 'ITH3'],
                   'nuts_3': ['FR101', 'ITH36']}


def test_generate_authors_and_institutes_no_authors(grid_lookup):
    authors, institutes = generate_authors_and_institutes(None, grid_lookup, grid_lookup)
    assert authors is None
    assert institutes == ['Universite De Vie', 'Universita De Vita', 
                          'University Of Life'] # original order


def test_generate_authors_and_institutes_good_authors(grid_lookup):
    mag_authors = [{'author_order': 1, 'affiliation_grid_id': 1, 'author_name': 'laura'}, 
                   {'author_order': 3, 'affiliation_grid_id': 2, 'author_name': 'clare'},
                   {'author_order': 2, 'affiliation_grid_id': 3, 'author_name': 'zoe'}]
    authors, institutes = generate_authors_and_institutes(mag_authors, grid_lookup, grid_lookup)
    assert authors == ['Laura', 'Zoe', 'Clare']  # sorted
    assert institutes == ['Universite De Vie', 'University Of Life', 
                          'Universita De Vita']  # sorted by author


def test_generate_authors_and_institutes_bad_authors(grid_lookup):
    mag_authors = [{'author_order': 1, 'affiliation_grid_id': 1, 'author_name': 'laura'}, 
                   {'affiliation_grid_id': 2, 'author_name': 'clare'},
                   {'author_order': 2, 'affiliation_grid_id': 3, 'author_name': 'zoe'}]
    authors, institutes = generate_authors_and_institutes(mag_authors, grid_lookup, grid_lookup)
    assert authors == ['Laura', 'Clare', 'Zoe']  # original order
    assert institutes == ['Universite De Vie', 'Universita De Vita', 
                          'University Of Life'] # original order
