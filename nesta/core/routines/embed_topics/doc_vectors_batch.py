'''
Raw text to document vectors
=============

Luigi batch to load the transform raw text to vectors using the Universal Sentence Encoder model from TensorHub.

The output vectors are stored in S3.
'''


import os
import datetime
import logging


import luigi
import boto3
import numpy as np
from sqlalchemy.sql.expression import func

from nesta.core.orms.gtr_orm import Projects
from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.luigihacks import autobatch, misctools
from nesta.core.orms.orm_utils import get_mysql_engine, db_session
from nesta.packages.misc_utils.batches import split_batches, put_s3_batch
# from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p

# bucket to store done keys from each batch task
S3 = boto3.resource('s3')
_BUCKET = S3.Bucket("nesta-production-intermediate")
DONE_KEYS = set(obj.key for obj in _BUCKET.objects.all())


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
    process_batch_size = luigi.IntParameter(default=1000)
    intermediate_bucket = luigi.Parameter()
    routine_id = luigi.Parameter()
    db_config_env = luigi.Parameter()

    def output(self):
        """Points to the output database engine where the task is marked as done. - For luigi updates table"""
        db_config_path = os.environ[self.db_config_env]
        db_config = misctools.get_config(db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "text2vec <dummy>"  # Note, not a real table
        update_id = "text2vectors_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def prepare(self):
        '''Prepare the batch job parameters'''
        db = 'dev' if self.test else 'production'
        engine = get_mysql_engine(self.db_config_env, 'mysqldb', db)

        with db_session(engine) as session:
            results = (session
                       .query(Projects.id,
                              func.length(Projects.abstractText))
                       .filter(Projects.abstractText is not None)
                       .distinct(Projects.abstractText)
                       .all())

        # Keep documents with a length larger than the 10th percentile.
        perc = np.percentile([r[1] for r in results], 10)
        all_ids = [r.id for r in results if r[1] >= perc]

        job_params = []
        for count, batch in enumerate(split_batches(all_ids, self.process_batch_size), start=1):
            # write batch of ids to s3
            key = f'text2vec-{self.routine_id}-{self.date}-{count}'
            batch_file = put_s3_batch(batch, self.intermediate_bucket, key)
            done = key in DONE_KEYS
            params = {
                "config": "mysqldb.config",
                "bucket": self.intermediate_bucket,
                "batch_file": batch_file,
                "db_name": db,
                "done": done,
                'outinfo': f"{key}",  # mark as done
                'test': self.test,
            }
            job_params.append(params)
            logging.info(params)
        return job_params

    def combine(self, job_params):
        """Touch the checkpoint"""
        self.output().touch()

#
# class RootTask(luigi.WrapperTask):
#     '''Add document vectors in S3.
#
#     Args:
#         date (datetime): Date used to label the outputs
#     '''
#     date = luigi.DateParameter(default=datetime.date.today())
#     production = luigi.BoolParameter(default=False)
#     process_batch_size = luigi.IntParameter(default=1000)
#
#     def requires(self):
#         '''Get the output from the batchtask'''
#         _routine_id = "{}-{}".format(self.date, self.production)
#         logging.getLogger().setLevel(logging.INFO)
#         return TextVectors(date=self.date,
#                            batchable=("~/nesta/nesta/core/"
#                                       "batchables/embed_topics/"),
#                            test=not self.production,
#                            db_config_env="MYSQLDB",
#                            process_batch_size=self.process_batch_size,
#                            intermediate_bucket="nesta-production-intermediate",
#                            job_def="py36_amzn1_image",
#                            job_name="text2vectors-%s" % self.date,
#                            job_queue="HighPriority",
#                            region_name="eu-west-2",
#                            env_files=[f3p("nesta/nesta/"),
#                                       f3p("config/mysqldb.config")],
#                            routine_id=_routine_id,
#                            poll_time=10,
#                            memory=4096,
#                            max_live_jobs=5)
