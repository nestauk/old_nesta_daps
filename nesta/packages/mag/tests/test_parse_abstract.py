import pytest

from nesta.packages.mag.parse_abstract import _uninvert_abstract
from nesta.packages.mag.parse_abstract import should_insert_full_stop
from nesta.packages.mag.parse_abstract import insert_full_stops
from nesta.packages.mag.parse_abstract import uninvert_abstract

PATH = 'nesta.packages.mag.parse_abstract.{}'

@pytest.fixture
def inverted_abstract():
    # This is taken directly from MAG
    import json
    import os
    this_dir = os.path.dirname(os.path.abspath(__file__))
    with open(f'{this_dir}/test_inverted_abstract.json') as f:
        return json.load(f)


@pytest.fixture
def uninverted_abstract():
    # This is taken directly from https://www.biorxiv.org/content/10.1101/406215v1
    import os
    this_dir = os.path.dirname(os.path.abspath(__file__))
    with open(f'{this_dir}/test_uninverted_abstract.txt') as f:
        return f.read()[:-1]  # strip off trailing newline

def test__uninvert_abstract(inverted_abstract):
    _uninverted_abstract = _uninvert_abstract(inverted_abstract)
    n_terms = sum(len(indexes) for term, indexes in inverted_abstract['InvertedIndex'].items())
    assert type(_uninverted_abstract) is list
    assert len(_uninverted_abstract) == n_terms
    assert all(term in _uninverted_abstract for term in inverted_abstract['InvertedIndex'].keys())


def test_should_insert_full_stop_shouldnt_work():
    shouldnt_work = ['Background Radiation', 
                     'background IR',
                     'background IRs',
                     'background: Radiation']
    for first_term, second_term in map(lambda x: x.split(), shouldnt_work):
        assert not should_insert_full_stop(first_term, second_term)
    

def test_should_insert_full_stop_should_work():
    should_work = ['background Radiation', 
                   'background Ir',
                   'background Irs']
    for first_term, second_term in map(lambda x: x.split(), should_work):
        assert should_insert_full_stop(first_term, second_term)
    

def test_insert_full_stops():
    test_text = ('This is an example of where a full stop should go '
                 'There should only be three full-stops BTW '
                 'Sometimes there could be other LOLs but not here')
    stopped_text = insert_full_stops(test_text.split())
    assert all(x in stopped_text for x in ('go.', 'BTW.', 'here.'))
    assert sum(x.endswith('.') for x in stopped_text) == 3


def test_uninvert_abstract(inverted_abstract, uninverted_abstract):
    assert uninvert_abstract(inverted_abstract) == uninverted_abstract
