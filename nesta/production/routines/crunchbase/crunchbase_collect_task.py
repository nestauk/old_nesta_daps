'''
Crunchbase data collection and processing
==================================

Luigi routine to collect Crunchbase data exports and load the data into MySQL.
'''

import luigi
import logging

from nesta.packages.crunchbase import get_csvs
from nesta.production.luigihacks import misctools
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.luigihacks import autobatch

import boto3

S3 = boto3.resource('s3')
_BUCKET = S3.Bucket("nesta-production-intermediate")
DONE_KEYS = set(obj.key for obj in _BUCKET.objects.all())


class CollectTask(autobatch.AutoBatchTask):
    '''Download tar file of csvs and load them into the MySQL server.

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
        db_config["table"] = "Crunchbase <dummy>"  # Note, not a real table
        update_id = "CrunchbaseCollectData_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def prepare(self):
        '''Prepare the batch job parameters'''
        # Iterate through all csvs
        job_params = []
        for csv in get_csvs():
            logging.info("Extracting table {}...".format(csv))
            table_name = ''.join(["crunchbase_{}", csv])
            done = table_name in DONE_KEYS
            params = {"table_name": table_name,
                      "config": "mysqldb.config",
                      "db_name": "production" if not self.test else "dev",
                      "outinfo": "s3://nesta-production-intermediate/%s" % table_name,
                      "done": done}
            job_params.append(params)
        return job_params

    def combine(self, job_params):
        '''Touch the checkpoint'''
        self.output().touch()
