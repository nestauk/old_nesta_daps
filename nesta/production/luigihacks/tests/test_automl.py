import mock
import pytest
import numpy as np
from copy import deepcopy

from nesta.production.luigihacks.automl import _MLTask
from nesta.production.luigihacks.automl import expand_pathstub
from nesta.production.luigihacks.automl import arange
from nesta.production.luigihacks.automl import expand_value_range
from nesta.production.luigihacks.automl import expand_hyperparameters
from nesta.production.luigihacks.automl import ordered_groupby
from nesta.production.luigihacks.automl import cascade_child_parameters
from nesta.production.luigihacks.automl import generate_uid

from nesta.production.luigihacks.automl import MLTask


@pytest.fixture
def mltask_kwargs():
    return dict(job_name='a name',
                s3_path_out='',
                batchable='',
                job_def='',
                job_queue='',
                region_name='')


def test__MLTask_is_MLTask(mltask_kwargs):
    mltask = _MLTask(**mltask_kwargs)
    assert isinstance(mltask, MLTask)
    assert mltask.__class__.__name__ != MLTask.__name__
    assert len(mltask.__class__.__name__) > 0


@mock.patch('nesta.production.luigihacks.misctools.'
            'find_filepath_from_pathstub', return_value='blah')
def test_expand_pathstub(mocked_fffp):
    assert type(expand_pathstub('nesta')) is str
    results = expand_pathstub(['nesta'])
    assert type(results) is list
    assert all(type(item) is str for item in results)


def test_arange():
    the_range = arange('np.arange(1,10,0.2)')
    assert the_range == list(np.arange(1,10,0.2))
    assert len(the_range) > 1
    assert type(the_range) is list


@mock.patch('nesta.production.luigihacks.automl.arange',
            return_value=[1,2,3])
def test_expand_value_range(mocked_arange):
    assert expand_value_range([1,2,3]) == [1,2,3]
    assert expand_value_range('np.arange(...)') == [1,2,3]
    assert expand_value_range(1.3) == [1.3]


def test_expand_value_range_random():
    with pytest.raises(NotImplementedError):
        expand_value_range('np.random(...)')

@mock.patch('nesta.production.luigihacks.automl.arange',
            return_value=[1,2,3])
def test_expand_hyperparameters(mocked_arange):
    row = {'hyperparameters': {'hyp1' : [1,2,3,4,5],
                               'hyp2' : 'np.arange(...)',
                               'hyp3' : 12.2},
           'other info': 'something else'}
    hyps = expand_hyperparameters(row.copy())
    assert len(hyps) == 5*3*1
    for hyp_set in hyps:
        assert hyp_set.keys() == row.keys()
        assert hyp_set['hyperparameters'].keys() == row['hyperparameters'].keys()
        assert all(type(v) is not list
                   for v in hyp_set['hyperparameters'].values())


def test_ordered_groupby():
    collection = [{'a': 22, 'b': 'blah'},
                  {'a': 2, 'b': 'bah'},
                  {'a': -34, 'b': 'yak'}]
    grouped = ordered_groupby(collection, 'a')
    for row, (group, _row) in zip(collection, grouped.items()):
        assert row['a'] == group
        assert row == _row[0]


def test_cascade_child_parameters():
    chain_parameters = {'child': [{'hyp1': -2, 'child':None}],
                        'parent': [{'hyp1': -2,
                                    'hyp2': 3,
                                    'child': 'child'},
                                   {'hyp1': -2,
                                    'hyp2': 3,
                                    'child': 'child'}],
                        'grandparent': [{'hyp1': 22,
                                         'hyp2': -2,
                                         'child': 'parent'}]}
    _chain_parameters = cascade_child_parameters(deepcopy(chain_parameters))
    uids = set(row['uid'] for rows in _chain_parameters.values()
               for row in rows)
    for job_name, rows in chain_parameters.items():
        _rows = _chain_parameters[job_name]
        # Number of rows should be the same, but the data
        # should have been modified
        assert len(_rows) >= len(rows)
        assert _rows != rows

        # Get rid of uid and assert all other keys are the same
        for _row in _rows:
            _row.pop('uid')
            for row in rows:
                assert all(k in _row.keys() for k in row)
        for row in rows:
            for _row in _rows:
                assert all(k in row.keys() for k in _row)

        # Assert that the child exists in the new keys
        if _row['child'] is not None:
            assert _row['child'] in uids

        # Get rid of child and check they're their otherwise the same
        for row in _rows:
            row.pop('child')
        for row in rows:
            if 'child' in row:
                row.pop('child')
        assert all(row in _rows for row in rows)


def test_generate_uid():
    row = {'hyperparameters': {'hyp1' : 'blah', 
                               'hyp2' : 23,
                               'hyp3' : 12.3,
                               'hyp4' : None}}
    uid = generate_uid('joel', row)
    assert type(uid) is str
    assert len(uid.split('.')) == len(row['hyperparameters']) + 1
