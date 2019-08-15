import datetime
import json
import os
import time
import logging
from itertools import chain

import numpy as np
import boto3
import luigi

from nesta.production.luigihacks import autobatch, s3
from nesta.production.luigihacks.misctools import get_config, find_filepath_from_pathstub
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.packages.companies_house.find_dissolved import generate_company_number_candidates

S3 = boto3.resource('s3')
S3_PREFIX = "s3://nesta-production-intermediate/CH_batch_params"
_BUCKET = S3.Bucket("nesta-production-intermediate")
DONE_KEYS = set(obj.key for obj in _BUCKET.objects.all())

MYSQLDB_ENV = "MYSQLDB"


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

        with open(os.environ["CH_API"], 'r') as f:
            api_key_l = f.read().split(',')
            

        prefixes = ['0', 'OC', 'LP', 'SC', 'SO', 'SL', 
                'NI', 'R', 'NC', 'NL']
        candidates = list(
                map(list, np.array_split(
                list(chain(*[generate_company_number_candidates(prefix) for prefix in prefixes])),
                len(api_key_l)
                ))
                )
        logging.info(f"candidates: {list(map(len, candidates))}")

        if self.test:
            candidates = candidates[:2]
            api_key_l = api_key_l[:2]

        job_params = []
        for api_key, batch_candidates in zip(api_key_l, candidates):
            key = api_key
            # Save candidate numbers to S3 by api_key
            inputinfo = f"{S3_PREFIX}_{key}_input"
            (S3.Object(*s3.parse_s3_path(inputinfo))
             .put(Body=json.dumps(batch_candidates)))

            params = {
                "CH_API_KEY": api_key,
                "db_name": "CompaniesHouse <dummy>",
                "inputinfo": inputinfo,
                "outinfo": f"{S3_PREFIX}_{key}",
                "test": self.test,
                "done": key in DONE_KEYS,
            }

            logging.info(params)
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
                                        "batchables/companies_house/"),
                             job_def="standard_image",
                             job_name="ch-batch-api-%s" % self.date,
                             env_files=[find_filepath_from_pathstub("nesta/nesta")],
                             job_queue="MinimalCpus",
                             region_name="eu-west-2",
                             poll_time=5)

    def output(self):
        pass

    def run(self):
        pass
