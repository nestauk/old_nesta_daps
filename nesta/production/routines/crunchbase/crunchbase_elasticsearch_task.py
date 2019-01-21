'''
Crunchbase data to elasticsearch
================================

Luigi routine to load the Crunchbase data from MYSQL into Elasticsearch.

Not all data is copied: organizations, categories and locations only. The data is
flattened and it is all stored in the same index.
'''

import boto3
from elasticsearch import Elasticsearch
import logging
import luigi
import os

from crunchbase_org_collect_task import OrgCollectTask
from nesta.packages.crunchbase.crunchbase_collect import org_batch_limits
from nesta.production.luigihacks import autobatch, misctools
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.orms.crunchbase_orm import Organization
from nesta.production.orms.orm_utils import db_session, get_mysql_engine, insert_data


S3 = boto3.resource('s3')
_BUCKET = S3.Bucket("nesta-production-intermediate")
DONE_KEYS = set(obj.key for obj in _BUCKET.objects.all())


class ElasticsearchTask(autobatch.AutoBatchTask):
    '''Download tar file of csvs and load them into the MySQL server.

    Args:
        date (datetime): Datetime used to label the outputs
        _routine_id (str): String used to label the AWS task
        db_config_path: (str) The output database configuration
    '''
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    db_config_env = luigi.Parameter()
    process_batch_size = luigi.IntParameter(default=10000)
    insert_batch_size = luigi.IntParameter(default=1000)

    def requires(self):
        yield OrgCollectTask(date=self.date,
                             _routine_id=self._routine_id,
                             test=self.test,
                             insert_batch_size=self.insert_batch_size,
                             db_config_env=self.db_config_env)

    def output(self):
        '''Points to the output database engine'''
        self.db_config_path = os.environ[self.db_config_env]
        db_config = misctools.get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Crunchbase to Elasticsearch <dummy>"  # Note, not a real table
        update_id = "CrunchbaseToElasticsearch_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)


    def prepare(self):
        # mysql setup
        db = 'dev' if self.test else 'production'
        engine = get_mysql_engine(self.db_config_env, 'mysqldb', db)

        # elasticsearch setup
        es_mode = 'crunchbase_orgs_dev' if self.test else 'crunchbase_orgs'
        es_config = misctools.get_config('elasticsearch.config', es_mode)
        es = Elasticsearch(es_config['external_host'], port=es_config['port'])

        if self.test:
            self.process_batch_size = 1000
            logging.warning(f"Batch size restricted to {self.process_batch_size} while in test mode")
        batches = org_batch_limits(engine, self.process_batch_size)
        job_params = []
        for batch_count, (start, end) in enumerate(batches):
            params = {'start_index': start,
                      'end_index': end,
                      'config': "mysqldb.config",
                      'db': db,
                      'outinfo': es_config['internal_host'],
                      'out_port': es_config['port'],
                      'out_index': es_config['index'],
                      'out_type': es_config['type'],
                      'done': es.exists(index=es_config['index'],
                                        doc_type=es_config['type'],
                                        id=end)
                      }
            print(params)
            job_params.append(params)
            batch_count += 1
            if self.test and batch_count > 1:
                logging.warning("Breaking after 2 batches while in test mode.")
                break

        logging.warning(f"Batch preparation completed, with {batch_count} batches")
        return job_params


        # '''Prepare the batch job parameters'''
        # tables = ['acquisitions',
        #           'degrees',
        #           'funding_rounds',
        #           'funds',
        #           'investment_partners',
        #           'investments',
        #           'investors',
        #           'ipos',
        #           'jobs',
        #           'people'
        #           ]

        # logging.info('Retrieving list of csvs in Crunchbase export')
        # all_csvs = get_csv_list()
        # logging.info(all_csvs)
        # if not all(table in all_csvs for table in tables):
        #     raise ValueError("Crunchbase export is missing one or more required tables")

        # db_name = 'dev' if self.test else 'production'

        # job_params = []
        # for table in tables:
        #     key = f"{table}_{db_name}"
        #     done = key in DONE_KEYS
        #     params = {"table": table,
        #               "config": "mysqldb.config",
        #               "db_name": db_name,
        #               "batch_size": self.insert_batch_size,
        #               "outinfo": f"s3://nesta-production-intermediate/{key}",
        #               "test": self.test,
        #               "done": done}
        #     job_params.append(params)
        #     logging.info(params)
        # return job_params

    def combine(self, job_params):
        '''Touch the checkpoint'''
        self.output().touch()
