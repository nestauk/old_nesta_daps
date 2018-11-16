'''
arXiv data collection and processing
==================================

Luigi routine to collect all data from the arXiv api and load it to MySQL.
'''

import luigi
import logging

from nesta.packages.arxiv.collect_arxiv import total_articles

from nesta.production.luigihacks import misctools
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.luigihacks import autobatch

import boto3

S3 = boto3.resource('s3')
_BUCKET = S3.Bucket("nesta-production-intermediate")
DONE_KEYS = set(obj.key for obj in _BUCKET.objects.all())
BATCH_SIZE = 10000


class CollectTask(autobatch.AutoBatchTask):
    '''Collect data from the arXiv api and dump the
    data in the MySQL server.

    Args:
        date (datetime): Datetime used to label the outputs
        _routine_id (str): String used to label the AWS task
        db_config_path: (str) The output database configuration
    '''
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    db_config_path = luigi.Parameter()

    def output(self):
        '''Points to the output database engine'''
        db_config = misctools.get_config(self.db_config_path, "mysqldb")
        db_config["database"] = "production" if not self.test else "dev"
        db_config["table"] = "arxiv <dummy>"  # Note, not a real table
        update_id = "ArxivCollectData_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def prepare(self):
        '''Prepare the batch job parameters'''
        job_params = []
        for batch_start in range(1, total_articles(), BATCH_SIZE):
            batch_done_key = '_'.join(['arxiv_collection_from', batch_start])
            done = batch_done_key in DONE_KEYS
            params = {"table_name": "arxiv_articles",
                      "config": "mysqldb.config",
                      "start_cursor": batch_start,
                      "end_cursor": batch_start + BATCH_SIZE,
                      "db_name": "production" if not self.test else "dev",
                      "outinfo": "s3://nesta-production-intermediate/%s" % batch_done_key,
                      "done": done}
            job_params.append(params)
        return job_params

    def combine(self, job_params):
        '''Touch the checkpoint'''
        self.output().touch()
