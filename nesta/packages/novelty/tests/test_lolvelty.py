import pytest
from unittest import mock
from nesta.packages.novelty.lolvelty import lolvelty

def test_lolvelty():
    es = mock.MagicMock()
    es.count.return_value = {'count': 100}
    # Very novel
    es.search.return_value = {'hits': {'hits':[{'_score':100},
                                               {'_score':5},
                                               {'_score':1},
                                               {'_score':1},
                                               {'_score':1}],
                                       'max_score': 100}}
    score = lolvelty(es, 'an_index', 'some_doc', [''])
    assert score > 200

    # Not novel at all
    es.search.return_value = {'hits': {'hits':[{'_score':1},
                                               {'_score':1},
                                               {'_score':1},
                                               {'_score':1},
                                               {'_score':1}],
                                       'max_score': 1}}
    score = lolvelty(es, 'an_index', 'some_doc', [''])
    assert score < 0

    # Somewhat novel
    es.search.return_value = {'hits': {'hits':[{'_score':10},
                                               {'_score':10},
                                               {'_score':1},
                                               {'_score':1},
                                               {'_score':1}],
                                       'max_score': 10}}
    score = lolvelty(es, 'an_index', 'some_doc', [''])    
    assert score > 0 and score < 200
