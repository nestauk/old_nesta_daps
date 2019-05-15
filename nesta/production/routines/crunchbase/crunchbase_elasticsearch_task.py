'''
Crunchbase data to elasticsearch
================================

Luigi routine to load the Crunchbase data from MYSQL into Elasticsearch.

Not all data is copied: organizations, categories and locations only. The data is
flattened and it is all stored in the same index.
'''

import boto3
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import logging
import luigi
import os

from crunchbase_parent_id_collect_task import ParentIdCollectTask
from nesta.packages.crunchbase.crunchbase_collect import all_org_ids
from nesta.packages.misc_utils.batches import split_batches, put_s3_batch
from nesta.production.luigihacks import autobatch
from nesta.production.luigihacks.misctools import get_config
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.orms.orm_utils import get_mysql_engine


S3 = boto3.resource('s3')
_BUCKET = S3.Bucket("nesta-production-intermediate")
DONE_KEYS = set(obj.key for obj in _BUCKET.objects.all())

def assert_correct_config(test, config, key):
    """Assert that config key and 'index' value are consistent with the
    running mode.

    Args:
        test (bool): Are we running in test mode?
        config (dict): Elasticsearch config file data.
        key (str): The name of the config key.
    """
    err_msg = "In test mode='{test}', config key '{key}' must end with '{suffix}'"
    if test and not key.endswith("_dev"):
        raise ValueError(err_msg.format(test=test, key=key, suffix='_dev'))
    elif not self.test and not index_name.endswith("_prod")
        raise ValueError(err_msg.format(test=test, key=key, suffix='_prod'))

    index = config[key]['index']
    if test and not index.endswith("_dev"):
        raise ValueError(f"In test mode the index '{key}' must end with '_dev'")


class ElasticsearchTask(autobatch.AutoBatchTask):
    '''Download tar file of csvs and load them into the MySQL server.

    Args:
        date (datetime): Datetime used to label the outputs
        _routine_id (str): String used to label the AWS task
        db_config_env (str): The output database envariable
        process_batch_size (int): Number of rows to process in a batch
        insert_batch_size (int): Number of rows to insert into the db in a batch
        intermediate_bucket (str): S3 bucket where the list of ids for each batch are
                                   written
    '''
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    db_config_env = luigi.Parameter()
    process_batch_size = luigi.IntParameter(default=10000)
    insert_batch_size = luigi.IntParameter()
    intermediate_bucket = luigi.Parameter()

    def requires(self):
        yield ParentIdCollectTask(date=self.date,
                                  _routine_id=self._routine_id,
                                  test=self.test,
                                  insert_batch_size=self.insert_batch_size,
                                  db_config_path=self.db_config_path,
                                  db_config_env=self.db_config_env)

    def output(self):
        '''Points to the output database engine'''
        self.db_config_path = os.environ[self.db_config_env]
        db_config = get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Crunchbase to Elasticsearch <dummy>"  # Note, not a real table
        update_id = "CrunchbaseToElasticsearch_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def prepare(self):
        # mysql setup
        self.database = 'dev' if self.test else 'production'
        engine = get_mysql_engine(self.db_config_env, 'mysqldb', self.database)

        # elasticsearch setup
        #es_mode = 'crunchbase_orgs_dev' if self.test else 'crunchbase_orgs_prod'
        es_config = get_config('elasticsearch.config', self.es_mode)
        assert_correct_config(self.test, self.es_mode, es_config)

        es = Elasticsearch(es_config['host'], port=es_config['port'], use_ssl=True)
        if self.test:
            self.process_batch_size = 1000
            logging.warning(f"Batch size restricted to {self.process_batch_size}"
                            " while in test mode")
        # Drop the index if required (must be in test mode to do this)
        _index = es_config['index']
        if self.reindex and self.test:
            es.indices.delete(index=_index)
        # Create the index if required
        if not es.indices.exists(index=_index):
            mapping = get_es_mapping('crunchbase', aliases='health_scanner')
            es.indices.create(index=_index, body=mapping)

        # get set of existing ids from elasticsearch via scroll
        query = {"_source": False}
        scanner = scan(es, query, index=_index, doc_type=es_config['type'])
        existing_ids = {s['_id'] for s in scanner}
        logging.info(f"Collected {len(existing_ids)} existing in Elasticsearch")

        # get set of all organisations from mysql
        all_orgs = all_org_ids(engine)
        logging.info(f"{len(all_orgs)} organisations in MYSQL")

        # remove previously processed
        orgs_to_process = (org for org in all_orgs if org not in existing_ids)

        job_params = []
        for count, batch in enumerate(split_batches(orgs_to_process,
                                                    self.process_batch_size), 1):
            # write batch of ids to s3
            batch_file = put_s3_batch(batch, self.intermediate_bucket, 'crunchbase_to_es')
            params = {"batch_file": batch_file,
                      "config": 'mysqldb.config',
                      "db_name": self.database,
                      "bucket": self.intermediate_bucket,
                      "done": False,
                      'outinfo': es_config['host'],
                      'out_port': es_config['port'],
                      'out_index': es_config['index'],
                      'out_type': es_config['type'],
                      "test": self.test
                      }
            logging.info(params)
            job_params.append(params)
            if self.test and count > 1:
                logging.warning("Breaking after 2 batches while in test mode.")
                break

        logging.warning(f"Batch preparation completed, with {len(job_params)} batches")
        return job_params

    def combine(self, job_params):
        '''Touch the checkpoint'''
        self.output().touch()
