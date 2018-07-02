from abc import ABC
from abc import abstractmethod
from collections import defaultdict
from luigihacks import batchclient
from subprocess import check_output
import time
import luigi


def command_line(command, verbose=False):
    out = check_output([command],shell=True)
    out_lines = out.decode("utf-8").split("\n")
    if verbose:
        for line in out_lines:
            print(">>>\t'", line.replace("\r",' '), "'")
    return out_lines[-2]


class AutoBatchTask(luigi.Task, ABC):
    '''A base class for automatically preparing and submitting batch tasks'''
    batchable = luigi.Parameter()
    job_def = luigi.Parameter()
    job_name = luigi.Parameter()
    job_queue = luigi.Parameter()
    region_name = luigi.Parameter()
    env_files = luigi.ListParameter(default=[])
    vcpus = luigi.IntParameter(default=1)
    memory = luigi.IntParameter(default=512)
    max_runs = luigi.IntParameter(default=None) # For testing
    timeout = luigi.IntParameter(default=21600)
    poll_time = luigi.IntParameter(default=60)
    success_rate = luigi.FloatParameter(default=0.75)    

    def run(self):
        # Generate the parameters for batches
        job_params = self.prepare()
        # Prepare the environment for batching
        env_files = " ".join(self.env_files)
        try:
            s3file_timestamp = command_line("nesta_prepare_batch "
                                            "{} {}".format(self.batchable, env_files))
        except CalledProcessError:
            raise BatchJobException("Invalid input or environment files")
        # Execute batch jobs
        self.execute(job_params, s3file_timestamp)
        # Combine the outputs
        self.combine(job_params)

    @abstractmethod
    def prepare(self):
        '''Must implement a method which returns a list of dicts
        Each dict of the output must contain the following keys:

        done (bool): indicating whether the job has already been finished
        outinfo (str): Text indicating e.g. the location of the output, for use
                       in the batch job and for `combine` method
        '''
        pass

    @abstractmethod
    def combine(self, job_params):
        '''Implement a method which writes the luigi.Target output'''
        pass

    def execute(self, job_params, s3file_timestamp):
        # Get AWS info to pass to the batch jobs
        aws_id = command_line("aws --profile default configure "
                              "get aws_access_key_id")
        aws_secret = command_line("aws --profile default configure "
                                  "get aws_secret_access_key")
        # Submit jobs
        batch_client = batchclient.BatchClient(poll_time=self.poll_time, 
                                               region_name=self.region_name)
        job_ids = []
        for i, params in enumerate(job_params):
            if params["done"]:
                continue
            # Break in case of testing
            if (self.max_runs is not None) and (i >= self.max_runs):
                break
            # Build a set of environmental variables to send
            # to the batch job
            env_variables = [{"name":"AWS_ACCESS_KEY_ID", "value":aws_id},
                             {"name":"AWS_SECRET_ACCESS_KEY", "value":aws_secret},
                             {"name":"BATCHPAR_S3FILE_TIMESTAMP", "value":s3file_timestamp}]
            for k,v in params.items():
                new_row = dict(name="BATCHPAR_{}".format(k), value=str(v))
                env_variables.append(new_row)
            # Add the environmental variables to the container overrides
            overrides = dict(environment=env_variables, 
                             memory=self.memory, vcpus=self.vcpus)
            id_ = batch_client.submit_job(jobDefinition=self.job_def,
                                          jobName=self.job_name,
                                          jobQueue=self.job_queue,
                                          timeout=dict(attemptDurationSeconds=self.timeout),
                                          containerOverrides=overrides)
            job_ids.append(id_)
        # Wait for jobs to finish
        self.monitor_batch_jobs(batch_client, job_ids)

    def monitor_batch_jobs(self, batch_client, job_ids):
        '''Wait for jobs to finish'''
        stats = defaultdict(int)  # Collection of failure vs total statistics
        done_jobs = set()
        while len(job_ids) - len(done_jobs) > 0:
            for id_ in job_ids:
                status = batch_client.get_job_status(id_)
                print(id_, status)
                if status not in ("SUCCEEDED","FAILED","RUNNING"):
                    continue
                stats[status] += 1
                if status == "RUNNING":
                    continue
                done_jobs.add(id_)
            # Check that the success rate is as expected
            if len(stats) > 0:
                self.assert_success(batch_client, job_ids, stats)
            # Wait before continuing
            print("Not finished yet...")
            time.sleep(self.poll_time)

    def assert_success(self, batch_client, job_ids, stats):
        total = sum(stats.values())
        failure_rate = stats["FAILED"] / total
        if failure_rate <= (1 - self.success_rate):
            return
        
        reason = "Exiting due to high failure rate: {}%".format(int(failure_rate*100))
        for job_id in job_ids:
            batch_client.terminate_job(jobId=job_id, reason=reason)
        raise batchclient.BatchJobException(reason)
