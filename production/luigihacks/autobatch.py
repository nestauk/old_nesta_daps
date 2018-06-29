from abc import ABC
from abc import abstractmethod
from luigihacks import BatchClient
from subprocess import check_output
import luigi


def command_line(command):
    out = check_output([command],shell=True)
    return out.decode("utf-8").split("\n")[-2]


class AutoBatchTask(ABC, luigi.Task):
    '''A base class for automatically preparing and submitting batch tasks'''
    job_def = luigi.Parameter()
    job_name = luigi.Parameter()
    job_queue = luigi.Parameter()
    vcpus = luigi.IntParameter(default=1)
    memory = luigi.IntParameter(default=512)
    max_runs = luigi.IntParameter(default=None) # For testing
    timeout = luigi.IntParameter(default=21600)
    poll_time = luigi.IntParameter(default=60)
    success_rate = luigi.FloatParameter(default=0.75)


    def __init__(self):
        super().__init__()
        self.aws_id = command_line("aws --profile default configure "
                                   "get aws_access_key_id")
        self.aws_secret = command_line("aws --profile default configure "
                                       "get aws_secret_access_key")                

    def run(self):
        # Prepare the environment for batching
        env_files = " ".join(self.env_files)
        try:
            s3file_timestamp = command_line("nesta_prepare_batch {}".format(env_files))
        except CalledProcessError:
            raise BatchJobException("Invalid input or environment files")
        # Generate the parameters for batches
        job_params = self.prepare()
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
        # Submit jobs
        batch_client = BatchClient(**self.aws_params)
        job_ids = []
        for i, params in enumerate(job_params):
            if params["done"]:
                continue
            # Break in case of testing
            if (max_runs is not None) and (i >= max_runs):
                break
            # Build a set of environmental variables to send
            # to the batch job
            env_variables = [{"name":"AWS_ACCESS_KEY_ID", "value":self.aws_id},
                             {"name":"AWS_SECRET_ACCESS_KEY", "value":self.aws_secret},
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
                                          timeout=self.timeout,
                                          containerOverrides=self.overrides)
            job_ids.append(id_)
        # Wait for jobs to finish
        self.monitor_batch_jobs(batch_client, job_ids)

    def monitor_batch_jobs(self)
        '''Wait for jobs to finish'''
        stats = defaultdict(int)  # Collection of failure vs total statistics
        done_jobs = set()
        while len(job_ids) - len(done_jobs) > 0:
            for id_ in job_ids:
                status = get_job_status(id_)
                if status not in ("SUCCEEDED","FAILED","RUNNING"):
                    continue
                stats[status] += 1
                if status == "RUNNING":
                    continue
                done_jobs.add(id_)

            # Check that the success rate is as expected
            self.assert_success(batch_client, job_ids, stats)
            # Wait before continuing
            print("Not finished yet...")
            time.sleep(self.poll_time)

    def assert_success(self, batch_client, job_ids, stats):
        total = sum(stats.values())
        success_rate = stats["FAILED"] / total
        if (total < 10) or (success_rate >= self.success_rate):
            return
        
        reason = "Exiting due to low success rate: {}".format(int(success_rate*100))
        for job_id in job_ids:
            batch_client.terminate_job(jobId=job_id, reason=reason)
        raise BatchJobException(reason)
