'''
Batch Example
=============

An example of building a pipeline with batched tasks.
'''

from nesta.production.luigihacks import autobatch
from nesta.production.luigihacks import s3
import luigi
import datetime
import json
import time
import numpy as np
from sqlalchemy.orm import sessionmaker
from nesta.production.orms.orm_utils import get_mysql_engine
from nesta.production.orms.gtr_orm import Projects
from sqlalchemy.sql.expression import func
from nesta.packages.misc_utils.batches import split_batches, put_s3_batch
# from nesta.packages.nlp_utils.text2vec import filter_documents

S3PREFIX = "s3://clio-text2vec/"


class TextVectors(autobatch.AutoBatchTask):
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
    process_batch_size = luigi.IntParameter(default=5000)
    intermediate_bucket = luigi.Parameter()
    routine_id = luigi.Parameter()

    # def requires(self):
    #     '''Gets the input data (json containing muppet name and age)'''
    #     return SomeInitialTask() # in my case, it doe3sn't require anything

    def output(self):
        '''Points to the S3 Target'''
        return s3.S3Target(S3PREFIX+"intermediate_output_%s.json" % self.date)

    def prepare(self):
        '''Prepare the batch job parameters'''

        db = 'dev' if self.test else 'production'
        engine = get_mysql_engine('MYSQLDB', 'mysqldb', db)
        Session = sessionmaker(engine)
        session = Session()

        results = (session
                   .query(Projects.id,
                          func.length(Projects.abstractText))
                   .filter(Projects.abstractText is not None)
                   .distinct(Projects.abstractText)
                   .all())

        # 10th percentile
        perc = np.percentile([r[1] for r in results], 10)
        all_ids = [r.id for r in results if r[1] >= perc]

        job_params = []
        for count, batch in enumerate(split_batches(all_ids, self.process_batch_size), 1):
            # write batch of ids to s3
            batch_file = put_s3_batch(batch, self.intermediate_bucket, self.routine_id)
            params = {
                "batch_file": batch_file,
                # "config": 'mysqldb.config',
                "db_name": database,
                "done": False,
                'outinfo': f'text2vec-{self.routine_id}-{self.date}-{count}',
                'test': self.test,
            }
            params.update(self.kwargs)

        # Add the mandatory `outinfo' and `done' fields
        for i, params in enumerate(job_params):
            params["outinfo"] = ("s3://nesta-production-intermediate/"
                                 "batch-example-{}-{}".format(self.date, i))
            params["done"] = s3fs.exists(params["outinfo"])
        return job_params

    # def combine(self, job_params):
    #     '''Combine the outputs from the batch jobs'''
    #     outdata = []
    #     for params in job_params:
    #         _body = s3.S3Target(params["outinfo"]).open("rb")
    #         _data = _body.read().decode('utf-8')
    #         outdata.append(json.loads(_data))
    #
    #     with self.output().open("wb") as f:
    #         f.write(json.dumps(outdata).encode('utf-8'))


class RootTask(luigi.Task):
    '''Add document vectors in S3.

    Args:
        date (datetime): Date used to label the outputs
    '''
    date = luigi.DateParameter(default=datetime.datetime.today())

    def requires(self):
        '''Get the output from the batchtask'''
        return SomeBatchTask(date=self.date,
                             batchable=("~/nesta/nesta/production/"
                                        "batchables/embed_topics/"),
                             job_def="standard_image",
                             job_name="batch-example-%s" % self.date,
                             job_queue="HighPriority",
                             region_name="eu-west-2",
                             poll_time=60)

    def output(self):
        '''Points to the S3 Target'''
        return s3.S3Target(S3PREFIX+"final_output_%s.json" % self.date)

    def run(self):
        '''Appends 'Muppet' the muppets' names'''

        time.sleep(10)
        instream = self.input().open('rb')
        data = json.load(instream)
        for row in data:
            row["name"] += " Muppet"

        with self.output().open('wb') as outstream:
            outstream.write(json.dumps(data).encode('utf8'))
