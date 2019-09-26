from unittest import mock
from nesta.core.batchables.nlp.vectorizer.run import term_counts
from nesta.core.batchables.nlp.vectorizer.run import optional
from nesta.core.batchables.nlp.vectorizer.run import merge_lists

PATH='nesta.core.batchables.nlp.vectorizer.run.{}'

def test_term_counts():
    _range = range(0, 100)
    term_indexes = [f'id{i}' for i in _range]
    term_names = {f'id{i}': f'name{i}' for i in _range}
    
    # Set the dct behaviour
    dct = mock.MagicMock()
    dct.doc2idx.return_value = term_indexes
    dct.__getitem__.side_effect = term_names.__getitem__
    
    # Non-binary
    _term_counts = term_counts(dct, {'dummy row'}, binary=False)
    assert sum(_term_counts.values()) == len(term_indexes)
    assert len(_term_counts) == len(term_names)

    # Binary
    _term_counts = term_counts(dct, {'dummy row'}, binary=True)
    assert sum(_term_counts.values()) == len(_term_counts) == len(term_names)
    

def test_optional_int():    
    for i in range(0, 100):
        key = f'VALUE{i}'
        with mock.patch.dict(PATH.format('os.environ'), {f'BATCHPAR_{key}':str(i)}):
            assert optional(key, default=None) == i    
    default = 'hello'
    assert optional('VALUE10000', default=default) == default        


def test_optional_str():
    for i in range(0, 100):
        key = f'VALUE{i}'
        with mock.patch.dict(PATH.format('os.environ'), {f'BATCHPAR_{key}':f'value{i}'}):
            assert optional(key, default=None) == f'value{i}'

            
def test_optional_dict():
    for i in range(0, 100):
        key = f'VALUE{i}'
        value = {f'value{i}':i}
        with mock.patch.dict(PATH.format('os.environ'), {f'BATCHPAR_{key}':str(value)}):
            assert optional(key, default=None) == value
            

def test_merge_lists():
    in_data = [[1,2,3],['a','b','c']]
    out_data = [1,2,3,'a','b','c']
    assert merge_lists(in_data) == out_data

def test_merge_lists_complicated():
    n = 1000
    in_data = [['1','2','3'],['a','b','c']]
    out_data = ['1','2','3','a','b','c']
    assert merge_lists(in_data) == out_data
    assert sorted(merge_lists(in_data*n)) == sorted(out_data*n)
