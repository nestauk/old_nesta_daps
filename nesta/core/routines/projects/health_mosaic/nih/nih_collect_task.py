'''
Data collection
===============

Luigi routine to collect NIH World RePORTER data
via the World ExPORTER data dump.
'''

import luigi
import datetime
import logging

from nesta.packages.nih.collect_nih import get_data_urls

from nesta.core.luigihacks import misctools
from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.luigihacks import autobatch

import boto3
import re

S3 = boto3.resource('s3')
_BUCKET = S3.Bucket("nesta-production-intermediate")
DONE_KEYS = set(obj.key for obj in _BUCKET.objects.all())

def exists(_class, **kwargs):
    statements = [getattr(_class, pkey.name) == kwargs[pkey.name]
                  for pkey in _class.__table__.primary_key.columns]
    return sql_exists().where(and_(*statements))


class CollectTask(autobatch.AutoBatchTask):
    '''Scrape CSVs from the World ExPORTER site and dump the
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
        db_config["table"] = "NIH <dummy>"  # Note, not a real table
        update_id = "NihCollectData_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def prepare(self):
        '''Prepare the batch job parameters'''
        # Iterate over all tabs
        job_params = []
        for i in range(0, 4):
            logging.info("Extracting table {}...".format(i))
            title, urls = get_data_urls(i)
            table_name = "nih_{}".format(title.replace(" ","").lower())
            for url in urls:
                done = url in DONE_KEYS
                params = {"table_name": table_name,
                          "url": url,
                          "config": "mysqldb.config",
                          "db_name": "production" if not self.test else "dev",
                          "outinfo": "s3://nesta-production-intermediate/%s" % url,
                          "done": done,
                          "entity_type": 'paper'}
                job_params.append(params)
        return job_params

    def combine(self, job_params):
        '''Touch the checkpoint'''
        self.output().touch()
