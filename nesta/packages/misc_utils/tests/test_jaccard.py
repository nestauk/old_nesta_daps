from nesta.packages.misc_utils.jaccard import _prepare_jaccard
from nesta.packages.misc_utils.jaccard import _jaccard
from nesta.packages.misc_utils.jaccard import jaccard
from nesta.packages.misc_utils.jaccard import best_jaccard
from nesta.packages.misc_utils.jaccard import _nested_intersection_score
from nesta.packages.misc_utils.jaccard import nested_jaccard
from nesta.packages.misc_utils.jaccard import fast_jaccard
from nesta.packages.misc_utils.jaccard import fast_nested_jaccard

import pytest

@pytest.fixture
def a():
    return ("1","2","3")

@pytest.fixture
def b():
    return ("3","a","51")

@pytest.fixture
def A():
    return {"1","2","3"}
    
@pytest.fixture
def B():
    return {"3","a","51"}

@pytest.fixture
def union():
    return {"1","2","3","a","51"}

@pytest.fixture
def itsct():
    return {"3"}
    

def test__prepare_jaccard(a, b, A, B, union, itsct):
    assert _prepare_jaccard(a, b) == (A, B, itsct, union)

def test__jaccard(union, itsct):
    assert _jaccard(itsct, union) == 1/5

def test_best_jaccard(a):
    bs = [(1,1,1), (1,"a","b"), ("1","2",4), (2,"3",5)]
    best, score = best_jaccard(a, bs)
    assert best == ("1","2",4)
    assert score == 2/4

def test_nested_intersection():
    _as = [(1,1,1), ("a","b"), (1,4)]
    bs = [(1,1,1), (1,"a","b"), (1,2,4), (2,"3",5)]
    assert _nested_intersection_score(_as.copy(), bs.copy(), nested_threshold=0) == [1, 2/3, 2/3]

    assert _nested_intersection_score(_as.copy(), bs.copy(), nested_threshold=0.5) == [1, 2/3, 2/3]

    assert _nested_intersection_score(_as.copy(), bs.copy(), nested_threshold=0.8) == [1]


def test_nested_jaccard(a, b):
    assert nested_jaccard(a, b, 0) == 1.5/4
    assert nested_jaccard(a, b, 0.4) == 1.5/4
    assert nested_jaccard(a, b, 0.7) == 1/5
    

def test_fast_jaccard():
    terms_to_index = [("the", "open", "university"), 
                      ("manchester", "university"), 
                      ("boston","analytics")]
    terms_to_query = [("the", "open", "university"),
                      ("munchester", "university"),
                      ("boston",)]
    jaccard_matches = fast_jaccard(terms_to_index, 
                                   terms_to_query)
    assert jaccard_matches == {("the", "open", "university"):
                               {("the", "open", "university"): 1},
                               ("manchester", "university"):
                               {("munchester", "university"): 1/3},
                               ("boston","analytics"):
                               {("boston",): 1/2}}
    
def test_fast_nested_jaccard():
    terms_to_index = [("the", "open", "university"), 
                      ("manchester", "university"), 
                      ("boston","analytics")]
    terms_to_query = [("the", "open", "university"),
                      ("munchester", "university"),
                      ("boston",)]
    jaccard_matches = fast_nested_jaccard(terms_to_index, 
                                          terms_to_query)
    assert jaccard_matches == {("the", "open", "university"):
                               {("the", "open", "university"): 1},
                               ("manchester", "university"):
                               {("munchester", "university"): 0.9},
                               ("boston","analytics"):
                               {("boston",): 1/2}}

