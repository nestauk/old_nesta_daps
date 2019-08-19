'''Automatic preparation, submission and consolidation
of AWS batch tasks; as a single Luigi Task.
'''

from abc import ABC
from abc import abstractmethod
from collections import defaultdict
from nesta.core.luigihacks import batchclient
from subprocess import check_output
from subprocess import CalledProcessError
import time
#import random
import luigi
from nesta.core.luigihacks.misctools import get_config
import logging
import os

# Define a global timeout, set to 95% of the timeout time
# in order to give the Luigi worker some grace
_config = get_config("luigi.cfg", "worker")


def command_line(command, verbose=False):
    '''Execute command line tasks and return the final output line.
    This is particularly useful for extracting the AWS access keys
    directly from the OS; as well as executing the environment
    preparation script (:code:`production/scripts/nesta_prepare_batch.sh`).
    '''
    # Execute the command and decode the output
    out = check_output([command], shell=True)
    out_lines = out.decode("utf-8").split("\n")
    if verbose:
        for line in out_lines:
            logging.info(f"{os.getpid()}: "
                         ">>>\t'{}'".format(line.replace("\r", ' ')))
    # The second last output is the actual final output
    # (ignoring the status code, which is the last output)
    return out_lines[-2]


class AutoBatchTask(luigi.Task, ABC):
    '''A base class for automatically preparing and submitting AWS batch tasks.

    Unlike regular Luigi :code:`Tasks`, which require the user
    to override the :code:`requires`, :code:`output` and :code:`run`
    methods, :code:`AutoBatchTask` instead effectively replaces
    :code:`run` with two new abstract methods: :code:`prepare`
    and :code:`combine`, which are repectively documented. With these abstract
    methods specified, :code:`AutoBatchTask` will automatically prepare,
    submit, and combine one batch task (specified in
    :code:`production.batchables`) per parameter set specified in the
    :code:`prepare` method. The :code:`combine` method will subsequently
    combine the outputs from the batch task.

    Args:
        batchable (str): Path to the directory containing the run.py batchable
        job_def (str): Name of the AWS job definition
        job_name (str): Name given to this AWS batch job
        job_queue (str): AWS batch queue
        region_name (str): AWS region from which to batch
        env_files (:obj:`list` of :obj:`str`, optional): List of names
                  pointing to local environmental files (for example local
                  imports or scripts) which should be zipped up with the
                  AWS batch job environment. Defaults to [].
        vcpus (int, optional): Number of CPUs to request for the AWS batch job.
              Defaults to 1.
        memory (int, optional): Memory to request for the AWS batch job.
               Defaults to 512 MiB.
        max_runs (int, optional): Number of batch jobs to run, which is useful
                 for testing a subset of the full pipeline, or making cost
                 predictions for AWS computing time. Defaults to `None`,
                 implying that all jobs should be run.
        poll_time (int, optional): Time in seconds between querying the AWS
                  batch job status. Defaults to 60.
        success_rate (float, optional): If the fraction of FAILED jobs exceeds
                     :code:`success_rate` then the entire Task, along with
                     any submitted AWS batch jobs, is killed. The fraction is
                     calculated with respect to any jobs with RUNNING,
                     SUCCEEDED or FAILED status. Defaults to 0.75.
    '''
    batchable = luigi.Parameter()
    job_def = luigi.Parameter()
    job_name = luigi.Parameter()
    job_queue = luigi.Parameter()
    region_name = luigi.Parameter()
    env_files = luigi.ListParameter(default=[])
    vcpus = luigi.IntParameter(default=1)
    memory = luigi.IntParameter(default=512)
    max_runs = luigi.IntParameter(default=None)  # For testing
    timeout = luigi.IntParameter(default=21600)
    poll_time = luigi.IntParameter(default=60)
    success_rate = luigi.FloatParameter(default=0.95)
    test = luigi.BoolParameter(default=True)
    max_live_jobs = luigi.IntParameter(default=25)
    worker_timeout = float('inf')
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failed_jobs = set()


    def run(self):
        '''DO NOT OVERRIDE THIS METHOD.

        An implementation of the :code:`Luigi.Task.run` method which is a
        wrapper around the :code:`prepare`, :code:`execute` and
        :code:`combine` methods. Instead of overriding this method, you
        should implement :code:`prepare` and :code:`combine` methods in
        your class.
        '''
        pid = os.getpid()
        self.TIMEOUT = time.time() + int(_config["timeout"])

        # Generate the parameters for batches
        job_params = self.prepare()
        if self.test:
            if len(job_params) > 2:
                job_params = job_params[0:2]
                logging.info(f"Test mode ({pid}): running {len(job_params)} jobs")

        # Prepare the environment for batching
        env_files = " ".join(self.env_files)
        try:
            if self.test:
                logging.info(f"Test mode ({pid}): Preparing batch")
            s3file_timestamp = command_line("nesta_prepare_batch "
                                            "{} {}".format(self.batchable,
                                                           env_files), self.test)
            if self.test:
                logging.info(f"Test mode ({pid}): Prepared batch")
        except CalledProcessError:
            raise batchclient.BatchJobException("Invalid input "
                                                "or environment files")
        # Execute batch jobs
        self.execute(job_params, s3file_timestamp)
        # Combine the outputs
        self.combine(job_params)

    @abstractmethod
    def prepare(self):
        '''You should implement a method which returns a :code:`list`
        of :code:`dict`, where each :code:`dict` corresponds to inputs
        to the batchable. Each row of the output must at least
        contain the following keys:

        - **done** (`bool`): indicating whether the job has already been
          finished.
        - **outinfo** (`str`): Text indicating e.g. the location of the output,
          for use in the batch job and for `combine` method

        Returns:
            :obj:`list` of :obj:`dict`
        '''
        pass

    @abstractmethod
    def combine(self, job_params):
        '''You should implement a method which collects the outputs specified by
        the **outinfo** key of :code:`job_params`, which is the output from the
        :code:`prepare` method. This method should finally write to the
        :code:`luigi.Target` output.

        Parameters:
            job_params (:obj:`list` of :obj:`dict`): The batchable job
                       parameters, as returned from the :code:`prepare` method.
        '''
        pass

    def execute(self, job_params, s3file_timestamp):
        ''' The secret sauce, which automatically submits and monitors
        the AWS batch jobs. Your AWS access key and id are automatically
        retrieved via the AWS CLI.

        Parameters:
            job_params (:obj:`list` of :obj:`dict`): The batchable job
                       parameters, as returned from the :code:`prepare` method.
                       Each job is submitted from every item in this
                       :code:`list`. Each `dict` key-value per is converted
                       into an environmental variable in the batch job, with
                       the variable
                       name formed from the key, prefixed by `BATCHPAR_`.
            s3file_timestamp (str): The timestamp of the batchable zip file
                             to be found on S3 by the AWS batch job.
        '''
        pid = os.getpid()
        # Get AWS info to pass to the batch jobs
        aws_id = command_line("aws --profile default configure "
                              "get aws_access_key_id", self.test)
        aws_secret = command_line("aws --profile default configure "
                                  "get aws_secret_access_key", self.test)

        # Build a set of environmental variables to send to the jobs
        env_variables = [{"name": "AWS_ACCESS_KEY_ID", "value": aws_id},
                         {"name": "AWS_SECRET_ACCESS_KEY",
                          "value": aws_secret},
                         {"name": "BATCHPAR_S3FILE_TIMESTAMP",
                          "value": s3file_timestamp}]
                         #{"name": "PYTHONIOENCODING", "value": "latin1"}]

        if self.test:
            logging.info(f"Test mode ({pid}): Got env variables")

        # Set up batch client, and check that we haven't 
        # already hit the time limit
        batch_client = batchclient.BatchClient(poll_time=self.poll_time,
                                               region_name=self.region_name)
        self._assert_timeout(batch_client, job_ids=[])
        if self.test:
            logging.info(f"Test mode ({pid}): Ready to batch")
        
        all_job_kwargs = []
        for i, params in enumerate(job_params):
            if params["done"]:
                continue
            # Break in case of testing
            if (self.max_runs is not None) and (i >= self.max_runs):
                break

            _env_variables = env_variables.copy()
            for k, v in params.items():
                new_row = dict(name="BATCHPAR_{}".format(k), value=str(v))
                _env_variables.append(new_row)
            # Add the environmental variables to the container overrides
            overrides = dict(environment=_env_variables,
                             memory=self.memory, vcpus=self.vcpus)
            job_kwargs = dict(jobDefinition=self.job_def,
                              jobName=self.job_name,
                              jobQueue=self.job_queue,
                              timeout=dict(attemptDurationSeconds=self.timeout),
                              containerOverrides=overrides)
            all_job_kwargs.append(job_kwargs)

        # Wait for jobs to finish
        self._run_batch_jobs(batch_client, all_job_kwargs)

    def _run_batch_jobs(self, batch_client, all_job_kwargs):
        '''Monitor AWS batch jobs until finished or failed.

        Parameters:
            batch_client (:obj:`BatchClient`)
            job_ids (:obj:`list` of :obj:`str`): List of AWS batch
                    job IDs to monitor.
        '''

        # Keep submitting until all submitted
        all_job_ids = set()
        done_job_ids = set()
        submitted_job_idxs = set()
        logging.info(f"{os.getpid()}: "
                     "{} jobs to run".format(len(all_job_kwargs)))
        while len(all_job_kwargs) > len(all_job_ids):
            # Get the number of live jobs
            running_job_ids = all_job_ids - done_job_ids
            n_live = len(running_job_ids)
            n_done = len(done_job_ids)
            n_left = len(all_job_kwargs) - n_done - n_live
            logging.info(f"{os.getpid()}: "
                         "{} jobs are live, "
                         "{} are finished, "
                         "and {} are yet to be submitted".format(n_live, n_done, n_left))
                                                 
            if n_live > 1:
                self._assert_timeout(batch_client, running_job_ids)
                self._assert_success(batch_client, all_job_ids, done_job_ids)
            # Submit some jobs until `self.max_live_jobs` reached
            for ijob, job_kwargs in enumerate(all_job_kwargs):
                if n_live >= self.max_live_jobs:
                    break
                if ijob in submitted_job_idxs:
                    continue
                # Submit a new job
                id_ = batch_client.submit_job(**job_kwargs)
                all_job_ids.add(id_)
                submitted_job_idxs.add(ijob)
                n_live += 1
            # Wait before continuing
            logging.info(f"{os.getpid()}: Not done submitting...")
            time.sleep(self.poll_time)

        # Wait until all finished
        running_job_ids = all_job_ids - done_job_ids
        while len(running_job_ids) > 0:
            running_job_ids = all_job_ids - done_job_ids
            self._assert_timeout(batch_client, running_job_ids)
            self._assert_success(batch_client, all_job_ids, done_job_ids)
            # Wait before continuing
            #logging.info("Not finished waiting...")
            time.sleep(self.poll_time)


    def _assert_success(self, batch_client, job_ids, done_jobs):
        '''Assert that success rate has not been breached.'''

        stats = defaultdict(int)  # Collection of failure vs total statistics
        # Check status for each job
        for id_ in job_ids:
            status = batch_client.get_job_status(id_)
            if id_ not in done_jobs:
                logging.debug(f"{os.getpid()}: "
                              "{} {}".format(id_, status))
            if status == "FAILED":
                self.failed_jobs.add(id_)
            if status not in ("SUCCEEDED", "FAILED", "RUNNING"):
                continue
            stats[status] += 1
            if status != "RUNNING":
                done_jobs.add(id_)

        # Ignore if jobs are simply stalling
        if len(stats) == 0:
            logging.info(f"{os.getpid()}: "
                         "No jobs are currently running")
            return

        # Calculate the failure rate
        total = sum(stats.values())
        failure_rate = stats["FAILED"] / total
        if failure_rate <= (1 - self.success_rate):
            return
        reason = "Exiting due to high failure rate: {}%".format(int(failure_rate*100))
        reason += "\nFailed jobs are: {}".format(self.failed_jobs)
        batch_client.hard_terminate(job_ids=job_ids, reason=reason)


    def _assert_timeout(self, batch_client, job_ids):
        '''Assert that timeout has not been breached.'''
        logging.info(f"{os.getpid()}: "
                     "{} seconds left".format(self.TIMEOUT - time.time()))
        if time.time() < self.TIMEOUT:
            return
        reason = f"{os.getpid()}: "
        reason += "Impending worker timeout, so killing live tasks"
        reason += "\nFailed jobs are: {}".format(self.failed_jobs)
        batch_client.hard_terminate(job_ids=job_ids, reason=reason)
