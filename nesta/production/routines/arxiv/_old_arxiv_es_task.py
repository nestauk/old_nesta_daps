'''
arXiv data to elasticsearch
================================

Luigi routine to load the arXiv data from MySQL into Elasticsearch.
'''

import boto3
from elasticsearch.helpers import scan
import logging
import luigi
import os

from nesta.packages.arxiv.collect_arxiv import all_article_ids
from nesta.production.routines.arxiv.arxiv_grid_task import GridTask
from nesta.packages.misc_utils.batches import split_batches, put_s3_batch
from nesta.production.luigihacks import autobatch
from nesta.production.luigihacks.estask import ElasticsearchTask
from nesta.production.luigihacks.misctools import get_config
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.orms.orm_utils import get_mysql_engine
from nesta.production.orms.orm_utils import setup_es


S3 = boto3.resource('s3')
_BUCKET = S3.Bucket("nesta-production-intermediate")
DONE_KEYS = set(obj.key for obj in _BUCKET.objects.all())

class ArxivElasticsearchTask(ElasticsearchTask):
    '''
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
    drop_and_recreate = luigi.BoolParameter(default=False)

    def requires(self):
        yield # MAG & GRID

    def output(self):
        '''Points to the output database engine'''
        self.db_config_path = os.environ[self.db_config_env]
        db_config = get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "arXiv to Elasticsearch <dummy>"  # Note, not a real table
        update_id = "arXivToElasticsearch_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def prepare(self):
        if self.test:
            self.process_batch_size = 1000
            logging.warning("Batch size restricted to "
                            f"{self.process_batch_size}"
                            " while in test mode")

        # MySQL setup
        self.database = 'dev' if self.test else 'production'
        engine = get_mysql_engine(self.db_config_env, 'mysqldb',
                                  self.database)

        # Elasticsearch setup
        es_mode = 'dev' if self.test else 'prod'
        es, es_config = setup_es(es_mode, self.test, self.drop_and_recreate,
                                 dataset='arxiv',
                                 aliases='arxlive')

        # Get set of existing ids from elasticsearch via scroll
        scanner = scan(es, query={"_source": False},
                       index=es_config['index'],
                       doc_type=es_config['type'])
        existing_ids = {s['_id'] for s in scanner}
        logging.info(f"Collected {len(existing_ids)} existing in "
                     "Elasticsearch")

        # Get set of all organisations from mysql
        all_articles = list(all_article_ids(engine))
        logging.info(f"{len(all_articles)} organisations in MySQL")

        # Remove previously processed
        arts_to_process = list(art for art in all_articles
                                   if art not in existing_ids)
        logging.info(f"{len(arts_to_process)} to be processed")

        job_params = []
        for count, batch in enumerate(split_batches(arts_to_process,
                                                    self.process_batch_size),
                                      1):
            logging.info(f"Processing batch {count} with size {len(batch)}")

            # write batch of ids to s3
            batch_file = put_s3_batch(batch, self.intermediate_bucket,
                                      'arxiv_to_es')
            params = {
                "batch_file": batch_file,
                "config": 'mysqldb.config',
                "db_name": self.database,
                "bucket": self.intermediate_bucket,
                "done": False,
                'outinfo': es_config['host'],
                'out_port': es_config['port'],
                'out_index': es_config['index'],
                'out_type': es_config['type'],
                'aws_auth_region': es_config['region'],
                'entity_type': 'arxiv',
                "test": self.test
            }

            logging.info(params)
            job_params.append(params)
            if self.test and count > 1:
                logging.warning("Breaking after 2 batches while in "
                                "test mode.")
                break

        logging.warning("Batch preparation completed, "
                        f"with {len(job_params)} batches")
        return job_params

    def combine(self, job_params):
        '''Touch the checkpoint'''
        self.output().touch()
