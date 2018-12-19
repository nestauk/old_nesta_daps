'''
Crunchbase data collection and processing
==================================

Luigi routine to collect Crunchbase data exports and load the data into MySQL.

Organizations, category_groups, org_parents and organization_descriptions should have
already been processed; this task picks up all other files to be imported.
'''

import boto3
import logging
import luigi
from random import shuffle

from nesta.packages.crunchbase.crunchbase_collect import get_csv_list
from nesta.production.luigihacks import autobatch, misctools
from nesta.production.luigihacks.mysqldb import MySqlTarget
from crunchbase_org_collect_task import OrgCollectTask


S3 = boto3.resource('s3')
_BUCKET = S3.Bucket("nesta-production-intermediate")
DONE_KEYS = set(obj.key for obj in _BUCKET.objects.all())


class NonOrgCollectTask(autobatch.AutoBatchTask):
    '''Download tar file of csvs and load them into the MySQL server.

    Args:
        date (datetime): Datetime used to label the outputs
        _routine_id (str): String used to label the AWS task
        db_config_path: (str) The output database configuration
    '''
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    db_config_path = luigi.Parameter()
    insert_batch_size = luigi.IntParameter(default=500)

    def requires(self):
        yield OrgCollectTask(date=self.date,
                             _routine_id=self._routine_id,
                             test=self.test,
                             insert_batch_size=self.insert_batch_size,
                             db_config_env='MYSQLDB')

    def output(self):
        '''Points to the output database engine'''
        db_config = misctools.get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Crunchbase <dummy>"  # Note, not a real table
        update_id = "CrunchbaseCollectNonOrgData_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def prepare(self):
        '''Prepare the batch job parameters'''
        tables = [#'acquisitions',
                  #'degrees',
                  'funding_rounds',
                  'funds',
                  'investment_partners',
                  'investments',
                  'investors',
                  'ipos',
                  'jobs',
                  'people'
                  ]

        logging.info('Retrieving list of csvs in Crunchbase export')
        all_csvs = get_csv_list()
        logging.info(all_csvs)
        if not all(table in all_csvs for table in tables):
            raise ValueError("Crunchbase export is missing one or more required tables")

        job_params = []
        if self.test:
            shuffle(tables)
        for table in tables:
            done = table in DONE_KEYS
            params = {"table": table,
                      "config": "mysqldb.config",
                      "db_name": "dev" if self.test else "production",
                      "batch_size": self.insert_batch_size,
                      "outinfo": "s3://nesta-production-intermediate/%s" % table,
                      "test": self.test,
                      "done": done}
            job_params.append(params)
        logging.info(job_params)
        return job_params

    def combine(self, job_params):
        '''Touch the checkpoint'''
        self.output().touch()
