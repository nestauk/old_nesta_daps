from unittest import mock

from nesta.packages.grid.grid_matcher import hashable_tokens
from nesta.packages.grid.grid_matcher import process_name
from nesta.packages.grid.grid_matcher import append_disputed_countries
from nesta.packages.grid.grid_matcher import generate_grid_lookups
from nesta.packages.grid.grid_matcher import _evaluate_matches
from nesta.packages.grid.grid_matcher import MatchEvaluator

PATH = "nesta.packages.grid.grid_matcher.{}"

def test_hashable_tokens():
    # Token and sort
    a = " tokenize my words please"
    _a = ('ease', 'eniz', 'keni', 'leas', 
          'my', 'nize', 'oken', 'ords', 
          'plea', 'toke', 'word')
    assert hashable_tokens(a) == _a

    # Excess spaces ignored
    b = " please   tokenize   my   words    "
    _b = _a  # Should be identical to first example
    assert hashable_tokens(b) == _b

    # Duplicate words ignored
    c = " my my please   chop up   my   words    "
    _c = ("chop", "ease", "leas", "my", 
          "ords", "plea", "up", "word")
    assert hashable_tokens(c) == _c


@mock.patch(PATH.format('hashable_tokens'))
def test_process_name(mocked_hashable_tokens):
    mocked_hashable_tokens.side_effect = lambda x: x
    a = "¡Tokenize my words, please!"
    _a = " tokenize my words  please "
    assert process_name(a) == _a


def test_append_disputed_countries():
    grid_ctrys = {'UGA', 'GBR', 'TWN'}
    _grid_ctrys = append_disputed_countries(grid_ctrys)
    assert _grid_ctrys == {'UGA', 'GBR', 'TWN', 'CHN'}

    grid_ctrys = {'USA', 'RKS', 'FRA'}
    _grid_ctrys = append_disputed_countries(grid_ctrys)
    assert _grid_ctrys == {'USA', 'RKS', 'FRA', 'SRB'}


def test__evaluate_matches_good_country():
    match_scores = {('a', 'name'): 0.8,
                    ('a', 'better', 'name'): 0.9}
    ctry_code = 'GBR'
    name_id_lookup = {('a', 'name'): {'grid.123'},
                      ('a', 'better', 'name'): {'grid.111'}}
    grid_ctry_lookup = {'grid.123': 'GBR',
                        'grid.111': 'UGA'}
    gids, score = _evaluate_matches(match_scores, ctry_code,
                                    name_id_lookup, grid_ctry_lookup,
                                    score_threshold=0.5)
    assert gids == {'grid.123'}
    assert score == 0.8


def test__evaluate_matches_perfect_match_bad_country():
    match_scores = {('a', 'name'): 1,
                    ('a', 'better', 'name'): 0.9}
    ctry_code = None
    name_id_lookup = {('a', 'name'): {'grid.123'},
                      ('a', 'better', 'name'): {'grid.111'}}
    grid_ctry_lookup = {'grid.123': 'GBR',
                        'grid.111': 'UGA'}
    gids, score = _evaluate_matches(match_scores, ctry_code,
                                    name_id_lookup, grid_ctry_lookup,
                                    score_threshold=0.1)
    assert gids == {'grid.123'}
    assert score == 1


def test__evaluate_matches_perfect_match_bad_country_long_name():
    match_scores = {('a', 'name'): 0.2,
                    ('a', 'better', 'name'): 1}
    ctry_code = 'GBR'
    name_id_lookup = {('a', 'name'): {'grid.123'},
                      ('a', 'better', 'name'): {'grid.111'}}
    grid_ctry_lookup = {'grid.123': 'GBR',
                        'grid.111': 'UGA'}
    gids, score = _evaluate_matches(match_scores, ctry_code,
                                    name_id_lookup, grid_ctry_lookup,
                                    score_threshold=0.5,
                                    long_name_threshold=3)
    assert gids == {'grid.111'}
    assert score == 1


def test__evaluate_matches_perfect_match_multimatch_bad_country():
    match_scores = {('a', 'name'): 1,
                    ('a', 'better', 'name'): 1}
    ctry_code = 'FRA'
    name_id_lookup = {('a', 'name'): {'grid.123'},
                      ('a', 'better', 'name'): {'grid.111'}}
    grid_ctry_lookup = {'grid.123': 'GBR',
                        'grid.111': 'UGA'}
    gids, score = _evaluate_matches(match_scores, ctry_code,
                                    name_id_lookup, grid_ctry_lookup,
                                    score_threshold=1,
                                    multimatch_threshold=2)
    assert gids == {'grid.111','grid.123'}
    assert score == 1


def test__evaluate_matches_perfect_match_multinat_bad_country():
    match_scores = {('a', 'name'): 0.8,
                    ('a', 'better', 'name'): 0.3}
    ctry_code = 'FRA'
    name_id_lookup = {('a', 'name'): {'grid.123','grid.123a','grid.123b'},
                      ('a', 'better', 'name'): {'grid.111'}}
    grid_ctry_lookup = {'grid.123': 'GBR',
                        'grid.123a': 'USA',
                        'grid.123b': 'CHN',
                        'grid.111': 'UGA'}
    gids, score = _evaluate_matches(match_scores, ctry_code,
                                    name_id_lookup, grid_ctry_lookup,
                                    score_threshold=0.8,
                                    multinat_threshold=3)
    assert gids == {'grid.123','grid.123a','grid.123b'}
    assert score == 0.8

def test__evaluate_matches_fails_match_multinat_bad_country():
    match_scores = {('a', 'name'): 0.8,
                    ('a', 'better', 'name'): 0.3}
    ctry_code = 'FRA'
    name_id_lookup = {('a', 'name'): {'grid.123','grid.123a','grid.123b'},
                      ('a', 'better', 'name'): {'grid.111'}}
    grid_ctry_lookup = {'grid.123': 'GBR',
                        'grid.123a': 'USA',
                        'grid.123b': 'CHN',
                        'grid.111': 'UGA'}
    gids, score = _evaluate_matches(match_scores, ctry_code,
                                    name_id_lookup, grid_ctry_lookup,
                                    score_threshold=0.8,
                                    multinat_threshold=10)
    assert gids == set()
    assert score == None


@mock.patch(PATH.format("generate_grid_lookups"))
def test_MatchEvaluator(mocked_generate_grid_lookups):
    apple_inc = process_name('apple inc')
    appl_inc = process_name('appl inc')
    apple = process_name('apple')
    google = process_name('google')
    doogle = process_name('doogle')
    nesta = process_name('nesta')
    nest = process_name('nest')
    all_grid_names = {apple_inc, apple, google, nesta}
    name_id_lookup = {apple_inc: {1}, 
                      apple: {2},
                      google: {3}, 
                      nesta: {4}}
    grid_ctry_lookup = {1: 'USA', 2: 'USA', 3: 'UGA', 4: 'FRA'}
    lookups = (all_grid_names, name_id_lookup, grid_ctry_lookup)
    mocked_generate_grid_lookups.return_value = lookups

    data = [{'id': "first", "names": {appl_inc}, 'iso3_code': 'USA'},
            {'id': 'second', "names": {google}, 'iso3_code': 'USA'},
            {'id': 'third', "names": {doogle}, 'iso3_code': 'UGA'},
            {'id': 'fourth', "names": {nest}, 'iso3_code': 'FRA'}]
    # Throw in some randomish noise
    for i in range(0, 5):
        row = {'id': i, "names": {('nesabgoogl'*i,'inc')}, 'iso3_code': None}
        data.append(row)

    # Again with looser thresholds
    me = MatchEvaluator(score_threshold=0.5, nested_threshold=0.6)
    matches = me.generate_matches(data)
    assert matches == {'first': {'grid_ids': {1}, 'score': 2/3}, 
                       'third': {'grid_ids': {3}, 'score': 8/9},
                       'fourth': {'grid_ids': {4}, 'score': 0.5}}

    # Again with tighter thresholds
    me = MatchEvaluator(score_threshold=0.5, nested_threshold=0.7)
    matches = me.generate_matches(data)
    assert matches == {'first': {'grid_ids': {1}, 'score': 2/3},
                       'third': {'grid_ids': {3}, 'score': 0.5},
                       'fourth': {'grid_ids': {4}, 'score': 0.5}}


    # Again with tighter thresholds
    me = MatchEvaluator(score_threshold=0.6, nested_threshold=0.7)
    matches = me.generate_matches(data)
    assert matches == {'first': {'grid_ids': {1}, 'score': 2/3}}
    
