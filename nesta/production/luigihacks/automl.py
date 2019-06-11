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

FLOAT = '([-+]?\d*\.\d+|\d+)' 
NP_ARANGE = f'np.arange\({FLOAT},{FLOAT},{FLOAT}\)' 

def upfill_child_parameters(chain_parameters):
    for job_name, rows in chain_parameters.items():
        for row in rows:
            uid = generate_uid(job_name, row)
            # Only parents after this point
            if "child" not in row:
                row['uid'] = uid
                continue
            # Upfill parameters to parents (including UID)
            child = row["child"]
            child_row = chain_parameters[child][0]
            row['uid'] = uid + '.' + child_row["uid"]
            for k, v in child_row.items():
                if k == "hyperparameters":
                    continue
                if k not in row:
                    row[k] = v
            row["child"] = child_row["uid"]
    return chain_parameters
            

def generate_uid(job_name, row):
    # Generate the UID
    uid = job_name.upper()
    try:
        hyps = row["hyperparameters"]
    except KeyError:
        pass
    else:
        uid += '.' + ".".join(f"{k}_{v}" for k, v in hyps.items())
    finally:
        return uid

def get_subbucket(s3_path):
    """Return subbucket path: <s3:pathto/subbucket_name>/keys

    Args:
        s3_path (str): S3 path string.
    Returns:
        subbucket_path (str): Path to the subbucket.
    """
    s3_bucket, s3_key = s3.parse_s3_path(s3_path)
    subbucket, _ = os.path.split(s3_key)
    return os.path.join(s3_bucket, subbucket)


def arange(x): 
    args = re.findall(NP_ARANGE, x.replace(' ',''))[0] 
    for i in  np.arange(*[float(arg) for arg in args]):
        yield i 

     
def each_value(value_range): 
    if type(value_range) is str:  
        if not value_range.startswith('np.arange'): 
            raise ValueError 
        value_range = arange(value_range) 
         
    try: 
        iter(value_range) 
    except TypeError: 
        value_range = [value_range] 
    finally: 
        for value in value_range: 
            yield value 


class DummyInputTask(luigi.ExternalTask):
    '''Dummy task acting as the single input data source'''
    s3_path_in = luigi.Parameter()

    def output(self):
        '''Points to the S3 Target'''
        return s3.S3Target(self.s3_path_in)


class MLTask(autobatch.AutoBatchTask):
    """A task which automatically spawns children if they exist.
    Note that other args are intended for autobatch.AutoBatchTask.

    Args:
        name (str): Name of the task instance, for book-keeping.
        s3_path_in (str): Path to the input data.
        s3_path_out (str): Path to the output data.
        batch_size (int): Size of batch chunks.
        n_batches (int): The number of batches to submit (alternative to :obj:`batch_size`)
        child (dict): Parameters to spawn a child task with.
        hyper (dict): Extra environmental variables to pass to the batchable.
    """
    name = luigi.Parameter()
    s3_path_in = luigi.Parameter()
    s3_path_out = luigi.Parameter()
    batch_size = luigi.IntParameter(default=None)
    n_batches = luigi.IntParameter(default=None)
    child = luigi.DictParameter()
    use_intermediate_inputs = luigi.BoolParameter(default=False)
    combine_outputs = luigi.BoolParameter(default=False)
    hyper = luigi.DictParameter(default={})


    def get_input_length(self):
        """Retrieve the length of the input, which is stored as the value
        of the output.length file."""
        f = s3.S3Target(f"{self.s3_path_in}.{self.test}.length").open('rb')
        total = json.load(f)
        f.close()
        return total


    def requires(self):
        """Spawns a child if one exists, otherwise points to a static input."""
        if len(self.child) == 0:
            logging.debug(f"{self.job_name}: Spawning DummyInput from {self.s3_path_in}")
            return DummyInputTask(s3_path_in=self.s3_path_in)
        logging.debug(f"{self.job_name}: Spawning MLTask with child = {self.child['name']}")
        return MLTask(**self.child)


    def output(self):
        """Points to the output"""
        if self.combine_outputs:
            return s3.S3Target(self.s3_path_out)
        return s3.S3Target(f"{self.s3_path_out}.{self.test}..length")


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

    def yield_batch(self, i, total):
        s3_key = self.s3_path_out
        # Mode 1: each batch is one of the intermediate inputs
        if self.use_intermediate_inputs:            
            first_key = 0
            last_key = -1
            S3 = boto3.resource('s3')
            subbucket_path = get_subbucket(self.s3_key_in)
            in_keys = S3.Bucket(subbucket_path).objects
            i = 0
            for in_key in in_keys.all():
                if not in_key.endswith("-{self.test}.json"):
                    continue
                out_key = self.s3_path_out.replace(".json", f"-{i}-{self.test}.json")
                yield first_index, last_index, in_key, out_key
                i += 1
            return

        # Mode 2: each batch is a subset of the single input
        total = self.set_batch_parameters()
        for i in range(0, self.n_batches):
            first_index, last_index = calculate_batch_indices(i, total)
            out_key = self.s3_path_out.replace(".json", (f"-{first_index}-"
                                                         f"{last_index}-"
                                                         f"{self.test}.json"))
            yield first_index, last_index, self.s3_key_in, out_key


    def prepare(self):
        """Prepare the batch task parameters"""
        # Generate the task parameters
        s3fs = s3.S3FS()
        job_params = []
        n_done = 0
        for first_index, last_index, in_key, out_key in self.yield_batch():
            # Fill the default params
            params = {"s3_path_in": in_key,
                      "first_index": first_index,
                      "last_index": last_index,
                      "outinfo": out_key,
                      "done": s3fs.exists(key)}

            # Add in any bonus paramters
            for k, v in self.hyper.items():
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
        outdata = []
        for params in job_params:
            _body = s3.S3Target(params["outinfo"]).open("rb")
            _data = _body.read().decode('utf-8')
            _outdata = json.loads(_data)
            if self.combine_outputs:
                outdata += _outdata
            size += len(_outdata)

        # Write the output
        logging.debug(f"{self.job_name}: Writing the output (length {len(outdata)})...")
        if self.combine_outputs:
            f = self.output().open("wb")
            f.write(json.dumps(outdata).encode('utf-8'))
            f.close()

        # Write the output length as well, for book-keeping
        f = s3.S3Target(f"{self.s3_path_out}.{self.test}.length").open("wb")
        f.write(str(size).encode("utf-8"))
        f.close()


class AutoMLTask(luigi.WrapperTask):
    """Parse and launch the MLTask chain based on an input
    configuration file.

    Args:
        s3_path_in (str): Path to the input data.
        s3_path_prefix (str): Prefix of all paths to the output data.
        task_chain_filepath (str): File path of the task chain configuration file.
        test (bool): Whether or not to run batch tasks in test mode.
    """
    s3_path_in = luigi.Parameter()
    s3_path_prefix = luigi.Parameter()
    task_chain_filepath = luigi.Parameter()
    test = luigi.BoolParameter(default=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        AutoMLTask.task_parameters = {}  # Container for keeping track of children

    def get_chain_parameters(self, env_keys=["batchable", "env_files"]):
        """Parse the chain parameters into a dictionary, and expand
        filepaths if specified.

        Args:
            env_keys (list): List (or list of lists) of partial
                             (note, not necessarily relative) filepaths to
                             expand into absolute filepaths. See :obj:`find_filepath_from_pathstub`
                             for more information.
        """
        with open(self.task_chain_filepath) as f:
            chain_parameters = json.load(f)

        #
        job_names = []

        # Expand hyperparameters if specified
        _key = 'hyperparameters'
        _chain_parameters = []
        child = None
        for row in chain_parameters:
            job_name = row['job_name']
            if job_name not in job_names:
                job_names.append(job_name)

            try:
                child = row['child']
            except KeyError:
                pass

            if _key not in row:
                _chain_parameters.append(row)
                child = job_name
                continue


            expanded_hyps = {k: each_value(values)
                             for k, values in row[_key].items()}
            hyp_values = itertools.product(*expanded_hyps.values())
            for values in hyp_values:
                hyps = {k: v for k, v in zip(expanded_hyps.keys(), values)}
                _row = deepcopy(row)
                _row[_key] = hyps
                _row['child'] = child
                _chain_parameters.append(_row)
            child = job_name
        chain_parameters = _chain_parameters

        # Expand filepaths if specified
        for row in chain_parameters:
            for k, v in row.items():
                if k not in env_keys:
                    continue
                # Expand from a list...
                if type(v) is list:
                    row[k] = [find_filepath_from_pathstub(_v) for _v in v]
                # ...or from a string (assumed)
                else:
                    row[k] = find_filepath_from_pathstub(v)

        # Group by job name
        chain_parameters = {job_name: [row for row in chain_parameters
                                       if row['job_name'] == job_name]
                            for job_name in job_names}
        return chain_parameters


    @staticmethod
    def append_hypers(params, output_name):
        """Append hyperparameters to the output string, useful for
        identifying the task chain which led to producing this output.

        Args:
            params (dict): Input job parameters.
            output_name (str): Output file prefix (so far).
        Returns:
            output_name (str): Output file prefix (extended).
        """
        job_name = params["job_name"].upper()
        #print(output_name, job_name, output_name.split("/")[-1].split("."))
        if job_name in output_name.split("/")[-1].split("."):
            return output_name
        output_name += job_name
        if "hyperparameters" in params:
            for k, v in params["hyperparameters"].items():
                output_name += f".{k}_{v}"
        output_name += "."
        return output_name

    @staticmethod
    def join_child_parameters(params, output_name=""):
        """Generate the output file prefix for this job by concatenating
        all child task job parameters. Useful for allowing users to
        identify the task chain which led to producing this output. Note that
        the function is recursive.

        Args:
            params (dict): Input job parameters.
            output_name (str): Output file prefix (so far).
        Returns:
            output_name (str): Output file prefix (extended).
        """
        output_name = AutoMLTask.append_hypers(params, output_name)
        if params["child"] != {}:
            return AutoMLTask.join_child_parameters(params["child"], output_name)
        return output_name

    def prepare_chain_parameters(self, chain_parameters):
        """Convert chain parameters into a form ready for MLTask.

        Args:
            chain_parameters (json): Parsed chain parameters.
        Returns:
            all_task_params (list): List of all MLTask parameters.
        """
        parameters = {}
        child_params = {}  # First task is childless by default
        all_task_params = {}
        s3_path_in = self.s3_path_in
        for task_params in chain_parameters:
            name = task_params["job_name"]

            # If task has a child, look up the child by name (MUST exist)
            if "child" in task_params:
                child_name = task_params.pop("child")
                child_params = all_task_params[child_name]

            # Generate the output path, starting with the parameters of this task...
            s3_path_out = AutoMLTask.append_hypers(task_params, 
                                                   f"{self.s3_path_prefix}/{name}/")
            # ... then append child parameters to the path...
            if child_params != {}:
                s3_path_in = child_params["s3_path_out"]
                s3_path_out += AutoMLTask.join_child_parameters(child_params)
            # ... and record whether this is a test or not
            s3_path_out += f"test_{self.test}.json"
            s3_path_out = s3_path_out.replace("//","/").replace("s3:/", "s3://")

            # Generate the parameters
            parameters = dict(s3_path_out=s3_path_out,
                              s3_path_in=s3_path_in,
                              child=child_params,
                              name=name,
                              test=self.test,
                              **task_params)

            # "Inherit" any missing parameters from the child
            for k in child_params:
                if k not in parameters:
                    parameters[k] = child_params[k]

            # Record the task parameters
            all_task_params[name] = parameters
            child_params = parameters  # The next task is assumed to be 
                                       # this task's parent,
                                       # unless specified otherwise.
        return all_task_params

    def requires(self):
        """Generate task parameters and yield MLTasks"""
        # Generate the parameters
        chain_parameters = self.get_chain_parameters()
        upfill_child_parameters(chain_parameters)
        
        # Launch jobs from the parameters
        for job_name, all_parameters in chain_parameters.items():
            AutoMLTask.task_parameters[job_name] = all_parameters
            for parameters in all_parameters:
                yield MLTask(**parameters)
