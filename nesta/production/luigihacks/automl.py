"""
automl
------

Automatic piping of Machine Learning tasks
(which are AutoBatchTasks) using
a configuration file. Data inputs and outputs are
chained according to S3Targets, although
obviously any batchable can access database data
if required.
"""

import luigi
from nesta.production.luigihacks import autobatch
from nesta.production.luigihacks.parameter import DictParameterPlus
from nesta.production.luigihacks import s3
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub
import os
import json
import logging
import math
import numpy as np
import itertools
from copy import deepcopy
import re
from collections import defaultdict
from collections import Counter
import boto3

FLOAT = r'([-+]?\d*\.\d+|\d+)'
NP_ARANGE = fr'np.arange\({FLOAT},{FLOAT},{FLOAT}\)'

def _MLTask(**kwargs):
    '''
    Factory function for dynamically creating py:class`MLTask`
    tasks with a specific class name. This helps to differentiate 
    py:class`MLTask` tasks from one another in the luigi scheduler.
    
    Args:
        kwargs (dict): All keyword arguments to construct an MLTask.
    Returns:
        MLTask object.
    '''
    _type = type(kwargs['job_name'].title(), (MLTask,), {})
    return _type(**kwargs)

def expand_pathstub(pathstub):
    """Expand the pathstub.

    Args:
        pathstub (list or str): A pathstub or list of pathstubs to expand.
    Returns:
        fullpath (list or str): A fullpath or list of fullpaths
    """
    # Expand from a list...
    if type(pathstub) is list:
        return [find_filepath_from_pathstub(_v) for _v in pathstub]
    # ...or from a string (assumed)
    else:
        return find_filepath_from_pathstub(pathstub)

def expand_hyperparameters(row):
    """Generate all hyperparameter combinations for this task.

    Args:
        row (dict): Row containing hyperparameter space to expand
    Returns:
        rows (list): List of dict to every combination of hyperparameters
    """
    expanded_hyps = {name: expand_value_range(values)
                     for name, values
                     in row.pop('hyperparameters').items()}
    hyp_names = expanded_hyps.keys()
    hyp_value_sets = itertools.product(*expanded_hyps.values())
    # Generate one row per hyperparameters combination
    return [dict(hyperparameters={name: value for name, value
                                  in zip(hyp_names, hyp_values)},
                 **row)
            for hyp_values in hyp_value_sets]

def ordered_groupby(collection, column):
    """Group collection by a column, maintaining the key
    order from the collection.

    Args:
        collection (list): List of flat dictionaries.
        column (str): Column (dict key) by which to group the list.
    Returns:
        grouped (dict): Dict of the column to subcollections.
    """
    # Figure out the group order
    group_order = []
    for row in collection:
        group = row[column]
        if group not in group_order:
            group_order.append(group)
    # Group by in order
    return {group: [row for row in collection
                    if row[column] == group]
            for group in group_order}


def cascade_child_parameters(chain_parameters):
    """Find upstream child parameters and cascade these
    to the parent.

    Args:
        chain_parameters (list): List of task parameters
    Returns:
        chain_parameters (list): List of task parameters, with children expanded.
    """
    _chain_parameters = defaultdict(list)
    for job_name, rows in chain_parameters.items():
        for row in rows:
            uid = generate_uid(job_name, row)
            # Only parents after this point
            if row["child"] is None:
                row['uid'] = uid
                _chain_parameters[job_name].append(deepcopy(row))
                continue
            # Cascade parameters to parents (including UID)
            child = row["child"]
            child_rows = _chain_parameters[child]
            for child_row in child_rows:
                _row = deepcopy(row)
                _row["child"] = child_row["uid"]
                _row['uid'] = uid + '.' + child_row["uid"]
                for k, v in child_row.items():
                    if k == "hyperparameters":
                        continue
                    if k not in _row:
                        _row[k] = v
                _chain_parameters[job_name].append(_row)
    return _chain_parameters

def generate_uid(job_name, row):
    """Generate the UID from the job name and children"""
    uid = job_name.upper()
    try:
        hyps = row["hyperparameters"]
    except KeyError:
        pass
    else:
        uid += '.' + ".".join(f"{k}_{v}" for k, v in hyps.items())
    finally:
        return uid

def deep_split(s3_path):
    """Return subbucket path: <s3:pathto/subbucket_name>/keys

    Args:
        s3_path (str): S3 path string.
    Returns:
        subbucket_path (str): Path to the subbucket.
    """
    s3_bucket, s3_key = s3.parse_s3_path(s3_path)
    subbucket, _ = os.path.split(s3_key)
    return s3_bucket, subbucket, s3_key


def arange(expression):
    """Expand and string representation of np.arange into a function call.

    Args:
        expression (str): String representation of :obj:`np.arange` function call.
    Yields:
        Result of :obj:`np.arange` function call.
    """
    args = re.findall(NP_ARANGE, expression.replace(' ',''))[0]
    return list(np.arange(*[float(arg) for arg in args]))

def expand_value_range(value_range_expression):
    """Expand the value range expression.

    Args:
        value_range_expression: Value range or expression to expand.
    Return:
        iterable.
    """
    if type(value_range_expression) is str:
        # Grid search
        if value_range_expression.startswith('np.arange'):
            value_range_expression = arange(value_range_expression)
        # Random search
        elif value_range_expression.startswith('np.random'):
            raise NotImplementedError('Random search space '
                                      'not implemented yet')
    # If not an iterable, make it an iterable
    try:
        iter(value_range_expression)
    except TypeError:
        value_range_expression = [value_range_expression]
    return value_range_expression


class MLTask(autobatch.AutoBatchTask):
    """A task which automatically spawns children if they exist.
    Note that other args are intended for autobatch.AutoBatchTask.

    Args:
        job_name (str): Name of the task instance, for book-keeping.
        #s3_path_in (str): Path to the input data.
        s3_path_out (str): Path to the output data.
        batch_size (int): Size of batch chunks.
        n_batches (int): The number of batches to submit (alternative to :obj:`batch_size`)
        child (dict): Parameters to spawn a child task with.
        hyper (dict): Extra environmental variables to pass to the batchable.
    """
    job_name = luigi.Parameter()
    s3_path_out = luigi.Parameter()
    input_task = luigi.TaskParameter(default=luigi.Task(),
                                     significant=False)
    input_task_kwargs = luigi.DictParameter(default={})
    batch_size = luigi.IntParameter(default=None)
    n_batches = luigi.IntParameter(default=None)
    child = DictParameterPlus(default=None)
    use_intermediate_inputs = luigi.BoolParameter(default=False)
    combine_outputs = luigi.BoolParameter(default=True)
    hyperparameters = luigi.DictParameter(default={})

    def requires(self):
        """Spawns a child if one exists, otherwise points
        to a static input."""
        if self.child is not None:
            msg = f"MLTask with child = {self.child['job_name']}"
            task = _MLTask(**self.child)
        else:
            msg = f"{str(self.input_task)} with {self.input_task_kwargs}"
            task = self.input_task(**self.input_task_kwargs)
        #else:
        #    msg = f"DummyInput from {self.s3_path_in}"
        #    task = DummyInputTask(s3_path_in=self.s3_path_in)
        logging.debug(f"{self.job_name}: Spawning {msg}")
        return task

    def output(self):
        """Points to the output"""
        if self.combine_outputs:
            return s3.S3Target(f"{self.s3_path_out}.json")
        return s3.S3Target(f"{self.s3_path_out}.length")

    @property
    def s3_path_in(self):
        target = self.input()
        return f's3://{target.s3_bucket}/{target.s3_key}'

    def get_input_length(self):
        """Retrieve the length of the input, which is stored as the value
        of the output.length file."""
        fname = self.s3_path_in
        if fname.endswith('.json'):
            fname = fname.replace('.json','')
        if not fname.endswith('.length'):
            fname = f"{fname}.length"
        f = s3.S3Target(fname).open('rb')
        total = json.load(f)
        f.close()
        return total

    def set_batch_parameters(self):
        # Assert that the batch size parameters aren't contradictory
        if self.batch_size is None and self.n_batches is None:
            raise ValueError("Neither batch_size for n_batches set")

        # Calculate the batch size parameters
        total = self.get_input_length()
        if self.n_batches is not None:
            self.batch_size = math.ceil(total/self.n_batches)
        else:
            n_full = math.floor(total/self.batch_size)
            n_unfull = int(total % self.batch_size > 0)
            self.n_batches = n_full + n_unfull

        logging.debug(f"{self.job_name}: Will use {self.n_batches} to "
                      f"process the task {total} "
                      f"with batch size {self.batch_size}")
        return total

    def calculate_batch_indices(self, i, total):
        """Calculate the indices of this batch"""
        first_index = i*self.batch_size
        last_index = (i+1)*self.batch_size
        if i == self.n_batches-1:
            last_index = total
        return first_index, last_index

    def yield_batch(self):
        s3_key = self.s3_path_out
        # Mode 1: each batch is one of the intermediate inputs
        if self.use_intermediate_inputs:
            first_idx = 0
            last_idx = -1
            s3_resource = boto3.resource('s3')
            bucket, subbucket, _ = deep_split(self.s3_path_in)
            in_keys = s3_resource.Bucket(bucket).objects
            i = 0
            for _in_key in in_keys.all():
                in_key = _in_key.key
                if not in_key.endswith(".json"):
                    continue
                if not in_key.startswith(subbucket):
                    continue
                out_key = f"{s3_key}-{i}.json"
                _in_key = f"s3://{bucket}/{in_key}"
                yield first_idx, last_idx, _in_key, out_key
                i += 1
        # Mode 2: each batch is a subset of the single input
        else:
            total = self.set_batch_parameters()
            for i in range(0, self.n_batches):
                first_idx, last_idx = self.calculate_batch_indices(i, total)
                out_key = (f"{s3_key}-{first_idx}_"
                           f"{last_idx}.json")
                yield first_idx, last_idx, self.s3_path_in, out_key

    def prepare(self):
        """Prepare the batch task parameters"""
        # Generate the task parameters
        s3fs = s3.S3FS()
        job_params = []
        n_done = 0
        for first_idx, last_idx, in_key, out_key in self.yield_batch():
            # Fill the default params
            done = s3fs.exists(out_key)
            params = {"s3_path_in": in_key,
                      "first_index": first_idx,
                      "last_index": last_idx,
                      "outinfo": out_key,
                      "done": done}
            # Add in any bonus paramters
            for k, v in self.hyperparameters.items():
                params[k] = v
            # Append and book-keeping
            n_done += int(done)
            job_params.append(params)
        # Done
        logging.debug(f"{self.job_name}: {n_done} of {len(job_params)} "
                      "have already been done.")
        return job_params

    def combine(self, job_params):
        """Combine output by concatenating results."""
        # Download and join
        logging.debug(f"{self.job_name}: Combining {len(job_params)}...")
        size = 0
        outdata = []
        for params in job_params:
            _body = s3.S3Target(params["outinfo"]).open("rb")
            _data = _body.read().decode('utf-8')
            _outdata = json.loads(_data)
            # Combine if required
            if len(job_params) == 1:
                outdata = _outdata
            elif self.combine_outputs:
                outdata += _outdata
            # Get the length of the data
            if type(_outdata) is not list:
                _outdata = _outdata['data']['rows']
            size += len(_outdata)

        # Write the output
        logging.debug(f"{self.job_name}: Writing the output "
                      f"(length {len(outdata)})...")
        if self.combine_outputs:
            f = self.output().open("wb")
            f.write(json.dumps(outdata).encode('utf-8'))
            f.close()

        # Write the output length as well, for book-keeping
        f = s3.S3Target(f"{self.s3_path_out}.length").open("wb")
        f.write(str(size).encode("utf-8"))
        f.close()


class AutoMLTask(luigi.Task):
    """Parse and launch the MLTask chain based on an input
    configuration file.

    Args:
        s3_path_in (str): Path to the input data.
        s3_path_prefix (str): Prefix of all paths to the output data.
        task_chain_filepath (str): File path of the task chain configuration file.
        test (bool): Whether or not to run batch tasks in test mode.
    """
    #s3_path_in = luigi.Parameter()
    input_task = luigi.TaskParameter()
    input_task_kwargs = luigi.DictParameter()
    s3_path_prefix = luigi.Parameter()
    task_chain_filepath = luigi.Parameter()
    test = luigi.BoolParameter(default=True)
    autobatch_kwargs = luigi.DictParameter(default={})
    maximize_loss = luigi.BoolParameter(default=False)
    gp_optimizer_kwargs = luigi.DictParameter(default={})

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        AutoMLTask.task_parameters = {}  # Container for keeping track of children

    def generate_seed_search_tasks(self, env_keys=["batchable", "env_files"]):
        """Generate task parameters, which could be fixed, grid or random.
        Note random not yet implemented.

        Parse the chain parameters into a dictionary, and expand
        filepaths if specified.

        Args:
            env_keys (list): List (or list of lists) of partial
                             (note, not necessarily relative) filepaths to
                             expand into absolute filepaths. See :obj:`find_filepath_from_pathstub`
                             for more information.
        """
        with open(self.task_chain_filepath) as f:
            _chain_parameters = json.load(f)

        # Expand filepaths, children and hyperparameters
        chain_parameters = []
        child = None
        for row in _chain_parameters:
            # Register a child if not specified
            if 'child' not in row:
                row['child'] = child
            # Expand filepaths
            for key in env_keys:
                if key in row:
                    row[key] = expand_pathstub(row[key])
            # Expand hyperparameters if any are specified
            try:
                _rows = expand_hyperparameters(row)
                # Sample ends and middle hyps if in test mode
                if self.test:
                    n_hyps = len(_rows)
                    if n_hyps > 3:
                        _rows = [_rows[0], _rows[-1],
                                 _rows[int((n_hyps+1)/2)]]
            except KeyError:
                chain_parameters.append(row)
            else:
                chain_parameters += _rows
            # This task is the proceeding task's child, by default
            child = row['job_name']

        # Group parameters by job name
        chain_parameters = ordered_groupby(collection=chain_parameters,
                                           column='job_name')
        # Impute missing parent information from the child
        chain_parameters = cascade_child_parameters(chain_parameters)
        # Done processing
        return chain_parameters

    def launch(self, chain_parameters):
        """Launch jobs from the parameters"""
        # Generate all kwargs for tasks
        test = f".TEST_{self.test}"
        kwargs_dict = {}
        all_children = set()
        for job_name, all_parameters in chain_parameters.items():
            AutoMLTask.task_parameters[job_name] = all_parameters
            for pars in all_parameters:
                s3_path_out = os.path.join(self.s3_path_prefix,
                                           pars.pop('uid'))+test
                s3_path_in = (None if pars['child'] is None
                              else os.path.join(self.s3_path_prefix,
                                                pars['child'])+test)
                all_children.add(s3_path_in)
                kwargs_dict[s3_path_out] = dict(s3_path_out=s3_path_out,
                                                s3_path_in=s3_path_in,
                                                test=self.test,
                                                **pars)
                if s3_path_in is None:
                    _kwargs = self.input_task_kwargs
                    kwargs_dict[s3_path_out]['input_task'] = self.input_task
                    kwargs_dict[s3_path_out]['input_task_kwargs'] = _kwargs


        # Launch the tasks
        for uid, kwargs in kwargs_dict.items():
            child_uid = kwargs.pop('s3_path_in')
            if child_uid is not None:
                kwargs['child'] = kwargs_dict[child_uid]
            # Only yield "pure" parents (those with no parents)
            if uid not in all_children:
                yield kwargs

    def requires(self):
        """Generate task parameters and yield MLTasks"""
        # Generate the parameters
        chain_parameters = self.generate_seed_search_tasks()
        # chain_parameters += self.generate_optimization_tasks() ## <-- blank optimisation tasks
        for kwargs in self.launch(chain_parameters):
            yield _MLTask(**kwargs, **self.autobatch_kwargs)

    def output(self):
        return s3.S3Target(f"{self.s3_path_prefix}.best")

    def run(self):
        # Get the UIDs for the final tasks
        final_task = list(AutoMLTask.task_parameters.keys())[-1]
        hypers = [generate_uid(final_task, row)
                  for row in AutoMLTask.task_parameters[final_task]]

        # Prepare AWS resources
        bucket_name, _, _ = deep_split(self.s3_path_prefix)
        bucket = boto3.resource('s3').Bucket(bucket_name)

        # 1 - 2*[1 OR 0] = [-1 OR 1]
        loss_sign = 1 - 2*int(self.maximize_loss)

        # Find the losse
        losses = {}
        for obj in bucket.objects.all():
            key = obj.key.split('/')[-1]
            if not key.endswith('json'):
                continue
            if not any(key.startswith(uid) for uid in hypers):
                continue
            js = json.load(obj.get()['Body'])
            losses[obj.key] = loss_sign * js['loss']

        # Least common = minimum loss
        best_key = Counter(losses).most_common()[-1][0]
        f = self.output().open("wb")
        f.write(best_key.encode('utf-8'))
        f.close()

        if len(self.gp_optimizer_kwargs) > 0:
            raise NotImplementedError('Gaussian Processes not implemented')
