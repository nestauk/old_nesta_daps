import datetime
import json
import os
import time

import boto3
import luigi

from nesta.production.luigihacks import autobatch, s3

S3 = boto3.resource('s3')
S3_PREFIX = "s3://nesta-dev/CH_batch_params"
_BUCKET = S3.Bucket("s3://nesta-dev/production-intermediate")
DONE_KEYS = set(obj.key for obj in _BUCKET.objects.all())

class SomeInitialTask(luigi.ExternalTask):
    '''Dummy task acting as the single input data source'''
    def output(self):
        '''Points to the S3 Target'''        
        return s3.S3Target(S3PREFIX+'input.json')


class CHBatchQuery(autobatch.AutoBatchTask):
    '''A set of batched tasks which increments the age of the muppets by 1 year.
    Args:
        date (datetime): Date used to label the outputs
        batchable (str): Path to the directory containing the run.py batchable
        job_def (str): Name of the AWS job definition
        job_name (str): Name given to this AWS batch job
        job_queue (str): AWS batch queue
        region_name (str): AWS region from which to batch
        poll_time (int): Time between querying the AWS batch job status
    '''
    date = luigi.DateParameter(default=datetime.datetime.today())

    def requires(self):
        pass

    def output(self):
        """ """
        db_config = get_config(os.environ[MYSQLDB_ENV], "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "CompaniesHouse <dummy>"
        update_id = f"CHQueryAPI_{self.date}"
        return MySqlTarget(update_id=update_id, **db_config)

    def prepare(self):
        '''Prepare the batch job parameters'''
        db_name = 'dev' if self.test else 'production'

        # TODO Generate candidate numbers - dummies for now
        candidates = ["01800000", "02800000"]
        for c in candidates.copy():
            for i in range(1, 5):
                c = c[:-1] + str(i)
                candidates.append(c)
        candidates = np.array_split(candidates, len(api_key_l))

        job_params = []
        for api_key, batch_candidates in zip(api_key_l, candidates):
            key = api_key
            # Save candidate numbers to S3 by api_key
            inputinfo = f"{S3_PREFIX}_{key}_input"
            (S3.Object(*s3.parse_s3_path(inputinfo))
             .put(Body=json.dumps(batch_candidates)))

            params = {
                "CH_API_KEY": api_key,
                "inputinfo": inputinfo,
                "outinfo": f"{S3_PREFIX}_{key}",
                "test": self.test,
                "done": key in DONE_KEYS
            }
            job_params.append(params)
        return job_params

    def combine(self, job_params):
        """ Touch the checkpoint """
        self.output().touch()

class RootTask(luigi.Task):
    """
    Args:
        date (`datetime`): Date used to label the outputs
    """
    date = luigi.DateParameter(default=datetime.datetime.today())

    def requires(self):
        '''Get the output from the batchtask'''
        return CHBatchQuery(date=self.date,
                             batchable=("~/nesta/nesta/production/"
                                        "batchables/companieshouse/"),
                             job_def="standard_image",
                             job_name="batch-example-%s" % self.date,
                             job_queue="MinimalCPUs",
                             region_name="eu-west-2",
                             poll_time=60)

    def output(self):
        pass

    def run(self):
        """ """

        time.sleep(10)
        instream = self.input().open('rb')
        data = json.load(instream)
        for row in data:
            row["name"] += " Muppet"

        with self.output().open('wb') as outstream:
            outstream.write(json.dumps(data).encode('utf8'))
