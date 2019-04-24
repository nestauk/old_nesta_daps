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


def get_subbucket(s3_path):
    """Return subbucket name: s3:pathto/<subbucket_name>/keys

    Args:
        s3_path (str): S3 path string.
    Returns:
        subbucket (str): Name of the subbucket.
    """
    _, s3_key = s3.parse_s3_path(s3_path)
    subbucket, _ = os.path.split(s3_key)
    return subbucket


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
        extra (dict): Extra environmental variables to pass to the batchable.
    """
    name = luigi.Parameter()
    s3_path_in = luigi.Parameter()
    s3_path_out = luigi.Parameter()
    batch_size = luigi.IntParameter(default=None)
    n_batches = luigi.IntParameter(default=None)
    child = luigi.DictParameter()
    #use_intermediate_inputs = luigi.BoolParameter(default=False)
    extra = luigi.DictParameter(default={})


    def get_input_length(self):
        """Retrieve the length of the input, which is stored as the value
        of the output.length file."""
        f = s3.S3Target(f"{self.s3_path_in}.length").open('rb')
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
        return s3.S3Target(self.s3_path_out)


    def prepare(self):
        """Prepare the batch task parameters"""

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

        # Generate the task parameters
        s3fs = s3.S3FS()
        s3_key = self.s3_path_out
        logging.debug(f"{self.job_name}: Will use {self.n_batches} to "
                      f"process the task {total} "
                      f"with batch size {self.batch_size}")
        job_params = []
        n_done = 0
        for i in range(0, self.n_batches):
            # Calculate the indices of this batch
            first_index = i*self.batch_size
            last_index = (i+1)*self.batch_size
            if i == self.n_batches-1:
                last_index = total

            # Generate the output path key
            key = s3_key.replace(".json", f"-{first_index}-{last_index}-{self.test}.json")
            # Fill the default params
            params = {"s3_path_in":self.s3_path_in,
                      "first_index": first_index,
                      "last_index": last_index,
                      "outinfo": key,
                      "done": s3fs.exists(key)}
            # Add in any bonus paramters
            for k, v in self.extra.items():
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
            outdata += json.loads(_data)

        # Write the output
        logging.debug(f"{self.job_name}: Writing the output (length {len(outdata)})...")
        f = self.output().open("wb")
        f.write(json.dumps(outdata).encode('utf-8'))
        f.close()

        # Write the output length as well, for book-keeping
        f = s3.S3Target(f"{self.s3_path_out}.length").open("wb")
        f.write(str(len(outdata)).encode("utf-8"))
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
        return chain_parameters


    @staticmethod
    def append_extras(params, output_name):
        """Append extra parameters to the output string, useful for
        identifying the task chain which led to producing this output.
        
        Args:
            params (dict): Input job parameters.
            output_name (str): Output file prefix (so far).
        Returns:
            output_name (str): Output file prefix (extended).
        """
        output_name += params["job_name"].upper()
        if "extra" in params:
            for k, v in params["extra"].items():
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
        output_name = AutoMLTask.append_extras(params, output_name)
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
            s3_path_out = AutoMLTask.append_extras(task_params, f"{self.s3_path_prefix}/{name}/")
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
            child_params = parameters  # The next task is assumed to be this task's parent, 
                                       # unless specified otherwise.

        return all_task_params

    def requires(self):
        """Generate task parameters and yield MLTasks"""
        chain_parameters = self.get_chain_parameters()
        all_task_params = self.prepare_chain_parameters(chain_parameters)
        for name, parameters in all_task_params.items():
            AutoMLTask.task_parameters[name] = parameters
            yield MLTask(**parameters)
