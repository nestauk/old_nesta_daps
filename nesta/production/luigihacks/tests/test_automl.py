import mock
import pytest
import numpy as np
from copy import deepcopy
import luigi

from nesta.production.luigihacks.automl import _MLTask
from nesta.production.luigihacks.automl import expand_pathstub
from nesta.production.luigihacks.automl import expand_envs
from nesta.production.luigihacks.automl import arange
from nesta.production.luigihacks.automl import expand_value_range
from nesta.production.luigihacks.automl import expand_hyperparams
from nesta.production.luigihacks.automl import ordered_groupby
from nesta.production.luigihacks.automl import cascade_child_params
from nesta.production.luigihacks.automl import generate_uid
from nesta.production.luigihacks.automl import bucket_filter
from nesta.production.luigihacks.automl import deep_split
from nesta.production.luigihacks.automl import subsample
from nesta.production.luigihacks.automl import MLTask
from nesta.production.luigihacks.automl import AutoMLTask

PATH='nesta.production.luigihacks.automl.{}'
MLPATH=PATH.format('MLTask.{}')
AMLPATH=PATH.format('AutoMLTask.{}')

@pytest.fixture
def js_losses():
    return [('a', {'loss': 10}), 
            ('b', {'loss': 1}), 
            ('c', {'loss': -2})]

class SomeTask(luigi.Task):
    pass

class Key:
    def __init__(self, k):
        self.key = k
    def get(self):
        return {'Body':None}

@pytest.fixture
def mltask_kwargs():
    return dict(job_name='a name',
                s3_path_out='s3://path/to/somewhere',
                batchable='',
                job_def='',
                job_queue='',
                region_name='')

@pytest.fixture
def mltask(mltask_kwargs):
    return MLTask(**mltask_kwargs)

@pytest.fixture
def automltask_kwargs():
    return dict(input_task=SomeTask,
                s3_path_prefix='s3://path/to/somewhere',
                task_chain_filepath='')

@pytest.fixture
def automltask(automltask_kwargs):
    return AutoMLTask(**automltask_kwargs)


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


@mock.patch(PATH.format('expand_pathstub'), return_value=True)
def test_expand_envs(_):
    row = {'key1':False, 'key2':False, 'key3':False, 'key4':False}
    env_keys = ['key1', 'key2']
    row = expand_envs(row, env_keys)
    assert len(row) == 4
    assert sum(row.values()) == 2

def test_arange():
    the_range = arange('np.arange(1,10,0.2)')
    assert the_range == list(np.arange(1,10,0.2))
    assert len(the_range) > 1
    assert type(the_range) is list


@mock.patch(PATH.format('arange'),
            return_value=[1,2,3])
def test_expand_value_range(mocked_arange):
    assert expand_value_range([1,2,3]) == [1,2,3]
    assert expand_value_range('np.arange(...)') == [1,2,3]
    assert expand_value_range(1.3) == [1.3]


def test_expand_value_range_random():
    with pytest.raises(NotImplementedError):
        expand_value_range('np.random(...)')

def test_expand_hyperparams_no_hyps():
    row = {'something':'else'}
    rows = expand_hyperparams(row)
    assert len(rows) == 1
    assert row == rows[0]

@mock.patch(PATH.format('arange'),
            return_value=[1,2,3])
def test_expand_hyperparams(mocked_arange):
    row = {'hyperparameters': {'hyp1' : [1,2,3,4,5],
                               'hyp2' : 'np.arange(...)',
                               'hyp3' : 12.2},
           'other info': 'something else'}
    hyps = expand_hyperparams(row.copy())
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


def test_cascade_child_params():
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
    _chain_parameters = cascade_child_params(deepcopy(chain_parameters))
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


def test_deep_split():
    s3_path = 's3://nesta-arxlive/automl/2019-07-07/COREX_TOPIC_MODEL.n_hidden_32.0.VECTORIZER.binary_True.min_df_0.001.NGRAM.TEST_False.json'
    s3_bucket, subbucket, s3_key = deep_split(s3_path)
    print(s3_bucket, subbucket, s3_key)
    assert '/' not in s3_bucket
    assert s3_key in s3_path
    assert subbucket in s3_key
    assert subbucket in s3_path
    assert s3_bucket in s3_path
    assert len(s3_key) > 0
    assert len(subbucket) > 0
    assert len(s3_bucket) > 0


def test_subsample():
    for i in range(1, 100):
        rows = list(range(0, i))
        _rows = subsample(rows)
        # Check no new data has been created
        assert all(a in rows for a in _rows)
        # Check that no duplication has occurred
        if i >= 3:
            assert len(_rows) == 3
        else:
            assert len(_rows) == len(rows)
        assert len(_rows) == len(set(_rows))

@mock.patch(PATH.format('json.load'))
@mock.patch(PATH.format('deep_split'), return_value=(None,None,None))
@mock.patch(PATH.format('boto3'))
def test_bucket_filter(mocked_boto3, _, json_load):
    # Mock some keys to return
    _keys = [Key(k) for k in ['a/first_key.json', 
                              'b/second_key.json',
                              'b/second_key.json',
                              'a/third_key.json', 
                              'a/third_key.other']] # Not JSON
    mocked_boto3.resource.return_value.Bucket.return_value.objects.all.return_value = _keys    
    assert len(list(bucket_filter('', ['first', 'third']))) == 2    
    assert len(list(bucket_filter('', ['second']))) == 2
    assert len(list(bucket_filter('', ['first', 'second', 'third']))) == 4


def test_MLTask_requires_no_child_no_input(mltask):
    with pytest.raises(ValueError):
        mltask.requires()


def test_MLTask_requires_no_child_yes_input(mltask_kwargs):
    mltask = MLTask(input_task=SomeTask, **mltask_kwargs)
    task = mltask.requires()
    assert type(task) == SomeTask

@mock.patch(PATH.format('_MLTask'))
def test_MLTask_requires_child(mocked_task, mltask_kwargs):
    mltask = MLTask(child={'job_name':'something'}, **mltask_kwargs)
    task = mltask.requires()
    assert mocked_task.call_count == 1
    assert task == mocked_task._mock_return_value

@mock.patch('nesta.production.luigihacks.s3.S3Target')
def test_MLTask_combine_output(mocked_target, mltask_kwargs):
    mltask = MLTask(combine_outputs=True, **mltask_kwargs)
    mltask.output()
    args, kwargs = mocked_target._mock_call_args
    assert args[0].endswith('.json')

@mock.patch('nesta.production.luigihacks.s3.S3Target')
def test_MLTask_no_combine_output(mocked_target, mltask_kwargs):
    mltask = MLTask(combine_outputs=False, **mltask_kwargs)
    mltask.output()
    args, kwargs = mocked_target._mock_call_args
    assert args[0].endswith('.length')

@mock.patch(MLPATH.format('input'))
def test_MLTask_s3_path_in(_, mltask):
    assert type(mltask.s3_path_in) is str
    assert len(mltask.s3_path_in) > 0


@mock.patch(MLPATH.format('s3_path_in'),
            new_callable=mock.PropertyMock)
def test_derive_file_length_path(mocked_path_in, mltask):
    mocked_path_in.return_value = 's3://a/b.json'
    assert mltask.derive_file_length_path() == 's3://a/b.length'


@mock.patch(MLPATH.format('s3_path_in'),
            new_callable=mock.PropertyMock)
def test_derive_file_length_path(mocked_path_in, mltask):
    path = 's3://a/b.length'
    mocked_path_in.return_value = path
    assert mltask.derive_file_length_path() == path


@mock.patch(MLPATH.format('s3_path_in'),
            new_callable=mock.PropertyMock)
def test_derive_file_length_path(mocked_path_in, mltask):
    path = 's3://a/b.something'
    mocked_path_in.return_value = path
    with pytest.raises(ValueError):
        mltask.derive_file_length_path()


@mock.patch(MLPATH.format('derive_file_length_path'))
@mock.patch('nesta.production.luigihacks.s3.S3Target')
@mock.patch('json.load', return_value=10)
def test_MLTask_get_input_length(_a, _b, _c, mltask):
    assert mltask.get_input_length() == 10


@mock.patch(MLPATH.format('derive_file_length_path'))
@mock.patch('nesta.production.luigihacks.s3.S3Target')
@mock.patch('json.load', side_effect=[None, 'a', {}])
def test_MLTask_get_input_length(_a, _b, _c, mltask):
    for i in range(0, 3):
        with pytest.raises(TypeError):
            mltask.get_input_length()


def test_MLTask_set_batch_parameters_bad_input(mltask):
    with pytest.raises(ValueError):
        mltask.set_batch_parameters()


@mock.patch(MLPATH.format('get_input_length'), return_value=1000)
def test_MLTask_set_batch_parameters_batch_size(_, mltask_kwargs):
    mltask = MLTask(batch_size=100, **mltask_kwargs)
    assert mltask.set_batch_parameters() == 1000
    assert batch_size == 100
    assert mltask.n_batches == 10

    mltask = MLTask(batch_size=900, **mltask_kwargs)
    mltask.set_batch_parameters()
    assert batch_size == 900
    assert mltask.n_batches == 2

    mltask = MLTask(batch_size=1000, **mltask_kwargs)
    mltask.set_batch_parameters()
    assert batch_size == 1000
    assert mltask.n_batches == 1

    mltask = MLTask(batch_size=1001, **mltask_kwargs)
    mltask.set_batch_parameters()
    assert batch_size == 1000
    assert mltask.n_batches == 1

@mock.patch(MLPATH.format('get_input_length'), return_value=1000)
def test_MLTask_set_batch_parameters_batch_size(_, mltask_kwargs):
    mltask = MLTask(n_batches=100, **mltask_kwargs)
    assert mltask.set_batch_parameters() == 1000
    assert mltask.batch_size == 10
    assert mltask.n_batches == 100

    mltask = MLTask(n_batches=1000, **mltask_kwargs)
    mltask.set_batch_parameters()
    assert mltask.batch_size == 1
    assert mltask.n_batches == 1000

    mltask = MLTask(n_batches=10000, **mltask_kwargs)
    mltask.set_batch_parameters()
    assert mltask.batch_size == 1
    assert mltask.n_batches == 1000


@mock.patch(MLPATH.format('get_input_length'), return_value=1000)
def test_MLTask_calculate_batch_indices(_, mltask_kwargs):
    mltask = MLTask(n_batches=100, **mltask_kwargs)
    with pytest.raises(ValueError):
        mltask.calculate_batch_indices(1, 10)

    total = mltask.set_batch_parameters()
    first_idx, last_idx = mltask.calculate_batch_indices(3, total)
    assert first_idx < last_idx
    assert last_idx - first_idx == mltask.batch_size

    first_idx, last_idx = mltask.calculate_batch_indices(99, total)
    assert last_idx == total

    with pytest.raises(ValueError):
        first_idx, last_idx = mltask.calculate_batch_indices(100,
                                                             total)


@mock.patch(MLPATH.format('s3_path_in'),
            new_callable=mock.PropertyMock)
@mock.patch(PATH.format('boto3'))
@mock.patch(PATH.format('deep_split'),
            return_value=(None, 'a', None)) # Mock subbucket = 'a'
def test_MLTask_yield_batch_use_intermediate(mocked_ds,
                                             mocked_boto3,
                                             mocked_s3_path,
                                             mltask_kwargs):
    # Mock some keys to return
    _keys = [Key(k) for k in ['a/first_key.json', # Good
                              'b/second_key.json', # Not in subbucket
                              'a/third_key.json', # Good
                              'a/third_key.other']] # Not json

    # Mock the full return iterable when iterating over objects
    # in a bucket
    mocked_boto3.resource.return_value.Bucket.return_value.objects.all.return_value = _keys

    # Test the yielding
    mltask = MLTask(input_task=SomeTask,
                    use_intermediate_inputs=True, **mltask_kwargs)
    out_keys = []
    for first_idx, last_idx, _in_key, out_key in mltask.yield_batch():
        # Indexes are always dummies in use_intermediate_inputs
        assert first_idx == 0
        assert last_idx == -1
        # Test keys look right
        assert _in_key.endswith('.json')
        assert out_key.endswith('.json')
        assert _in_key != mocked_s3_path
        out_keys.append(out_key)
    assert len(out_keys) == len(set(out_keys))
    assert len(out_keys) == 2

@mock.patch(MLPATH.format('s3_path_in'),
            new_callable=mock.PropertyMock)
@mock.patch(MLPATH.format('get_input_length'), return_value=1000)
def test_MLTask_yield_batch_not_use_intermediate(mocked_len,
                                                 mocked_s3_path,
                                                 mltask_kwargs):
    mltask = MLTask(input_task=SomeTask, n_batches=100,
                    use_intermediate_inputs=False, **mltask_kwargs)
    out_keys = []
    previous_first_idx = -1
    previous_last_idx = -1
    for first_idx, last_idx, _in_key, out_key in mltask.yield_batch():
        assert first_idx < last_idx
        assert first_idx > previous_first_idx
        assert last_idx > previous_last_idx
        assert _in_key == mocked_s3_path()
        out_keys.append(out_key)
        previous_first_idx = first_idx
        previous_last_idx = last_idx
    assert len(out_keys) == mltask.n_batches

@mock.patch(MLPATH.format('yield_batch'),
            return_value=[(None,None,None,None)]*126)
@mock.patch(PATH.format('s3'))
def test_MLTask_prepare(mocked_s3, mocked_yield_batch, mltask_kwargs):
    mocked_s3.S3FS.return_value.exists.side_effect = [True,False,True]*int(126/3)
    mltask = MLTask(hyperparameters={'a':20, 'b':30},
                    **mltask_kwargs)
    job_params = mltask.prepare()

    # Check that the numbers add up
    assert len(job_params) == 126
    assert sum(p['done'] for p in job_params) == int(126*2/3) ## 2/3 are True
    # Check the hyperparameters are there
    for p in job_params:
        assert 'a' in p.keys()
        assert 'b' in p.keys()

@mock.patch(PATH.format('s3'))
@mock.patch(PATH.format('json'))
def test_MLTask_combine_outputs_many(mocked_json, mocked_s3, mltask):
    param = mock.MagicMock()
    param.__getitem__.side_effect = None
    job_params = [param]*156
    mocked_json.loads.return_value = [{'key': 'value'}]*124
    size, outdata = mltask.combine_all_outputs(job_params)
    assert len(outdata) == size
    assert size == 156*124

@mock.patch(PATH.format('s3'))
@mock.patch(PATH.format('json'))
def test_MLTask_combine_outputs_one(mocked_json, mocked_s3, mltask):
    param = mock.MagicMock()
    param.__getitem__.side_effect = None
    job_params = [param]
    mocked_json.loads.return_value = {'data':{'rows':list(range(0, 126))}}
    size, outdata = mltask.combine_all_outputs(job_params)
    assert len(outdata) == 1
    assert size == 126


@mock.patch("builtins.open")
@mock.patch(PATH.format('json.load'))
@mock.patch(PATH.format('expand_envs'), side_effect=lambda x,_:x)
@mock.patch(PATH.format('expand_hyperparams'), side_effect=lambda x:[x])
@mock.patch(PATH.format('subsample'), side_effect=lambda x:x)
@mock.patch(PATH.format('ordered_groupby'), side_effect=lambda x,_:x)
@mock.patch(PATH.format('cascade_child_params'), side_effect=lambda x:x)
def test_AutoMLTask_generate_seed_search_tasks(ccp, og, ss, eh,
                                               ee, json_load, _open,
                                               automltask):
    json_load.return_value = [{'job_name': 'input_task'},
                              {'job_name': 'second_task'},
                              {'job_name': 'third_task'},
                              {'job_name': 'final_task',
                               'child': 'second_task'}]
    chain_params = automltask.generate_seed_search_tasks()
    assert all('child' in params for params in chain_params)
    assert chain_params[0]['child'] is None
    assert chain_params[1]['child'] == 'input_task'
    assert chain_params[2]['child'] == 'second_task'
    assert chain_params[3]['child'] == 'second_task'


def test_AutoMLTask_launch(automltask):
    pars = {'input_task': [{'uid': 'IT', 'child': None}],
            'second_task': [{'uid': 'ST', 'child': 'IT'}],
            'third_task': [{'uid':'TT', 'child': 'ST'}],
            'final_task': [{'uid':'FT0', 'child': 'ST'},
                           {'uid':'FT1', 'child': 'ST'}]}
    n = 0
    for kwargs in automltask.launch(pars):
        n += 1
        # Check all children have been assigned
        _kwargs = kwargs['child']
        assert type(_kwargs) is dict
        while type(_kwargs) is dict:
            assert _kwargs is not None
            _kwargs = _kwargs['child']
    # Expect only the final three since they 
    # are not children
    assert n == 3

@mock.patch(PATH.format('bucket_filter'))
def test_AutoMLTask_extract_losses(bf, automltask, js_losses):
    bf.return_value = js_losses
    losses = automltask.extract_losses([])
    assert min(losses.values()) == min(js['loss'] for _, js in js_losses)


@mock.patch(PATH.format('bucket_filter'))
def test_AutoMLTask_extract_losses_max(bf, automltask_kwargs, js_losses):
    bf.return_value = js_losses
    automltask = AutoMLTask(maximize_loss=True, **automltask_kwargs)
    losses = automltask.extract_losses([])
    assert min(losses.values()) == -max(js['loss'] for _, js in js_losses)

