'''
Sql2EsTask
==========

Task for piping data from MySQL to Elasticsearch
'''

import logging
import luigi
import os

from nesta.packages.misc_utils.batches import split_batches, put_s3_batch
from nesta.core.luigihacks import autobatch
from nesta.core.luigihacks.misctools import get_config
from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.orms.orm_utils import get_mysql_engine
from nesta.core.orms.orm_utils import setup_es
from nesta.core.orms.orm_utils import db_session
from nesta.core.orms.orm_utils import get_es_ids


class Sql2EsTask(autobatch.AutoBatchTask):
    '''Launches batch tasks to pipe data from MySQL to Elasticsearch.

    Args:
        date (datetime): Datetime used to label the outputs.
        routine_id (str): String used to label the AWS task.
        intermediate_bucket (str): Name of the S3 bucket where to store the batch ids.
        db_config_env (str): The output database envariable.
        process_batch_size (int): Number of rows to process in a batch.
        drop_and_recreate (bool): If in test mode, drop and recreate the ES index?
        dataset (str): Name of the elasticsearch dataset.
        id_field (SqlAlchemy selectable attribute): The ID field attribute.
        entity_type (str): Name of the entity type to label this task with.
        kwargs (dict): Any other job parameters to pass to the batchable.
    '''
    date = luigi.DateParameter()
    routine_id = luigi.Parameter()
    intermediate_bucket = luigi.Parameter()
    db_config_env = luigi.Parameter()
    process_batch_size = luigi.IntParameter(default=10000)
    drop_and_recreate = luigi.BoolParameter(default=False)
    aliases = luigi.Parameter(default=None)
    dataset = luigi.Parameter()
    id_field = luigi.Parameter()
    entity_type = luigi.Parameter()
    kwargs = luigi.DictParameter(default={})

    def output(self):
        '''Points to the output database engine'''
        self.db_config_path = os.environ[self.db_config_env]
        db_config = get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = f"{self.routine_id} <dummy>"  # Note, not a real table
        update_id = f"{self.routine_id}_{self.date}"
        return MySqlTarget(update_id=update_id, **db_config)

    def prepare(self):
        if self.test:
            self.process_batch_size = 1000
            logging.warning("Batch size restricted to "
                            f"{self.process_batch_size}"
                            " while in test mode")

        # MySQL setup
        database = 'dev' if self.test else 'production'
        engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)

        # Elasticsearch setup
        es_mode = 'dev' if self.test else 'prod'
        es, es_config = setup_es(es_mode, self.test, 
                                 self.drop_and_recreate,
                                 dataset=self.dataset,
                                 aliases=self.aliases)

        # Get set of existing ids from elasticsearch via scroll
        existing_ids = get_es_ids(es, es_config)
        logging.info(f"Collected {len(existing_ids)} existing in "
                     "Elasticsearch")

        # Get set of all organisations from mysql
        with db_session(engine) as session:
            result = session.query(self.id_field).all()
            all_ids = {r[0] for r in result}
        logging.info(f"{len(all_ids)} organisations in MySQL")

        # Remove previously processed
        ids_to_process = (org for org in all_ids
                          if org not in existing_ids)

        job_params = []
        for count, batch in enumerate(split_batches(ids_to_process,
                                                    self.process_batch_size),
                                      1):
            # write batch of ids to s3
            batch_file = put_s3_batch(batch, self.intermediate_bucket, self.routine_id)
            params = {
                "batch_file": batch_file,
                "config": 'mysqldb.config',
                "db_name": database,
                "bucket": self.intermediate_bucket,
                "done": False,
                'outinfo': es_config['host'],
                'out_port': es_config['port'],
                'out_index': es_config['index'],
                'out_type': es_config['type'],
                'aws_auth_region': es_config['region'],
                'entity_type': self.entity_type,
                'test': self.test,
                'routine_id': self.routine_id
            }
            params.update(self.kwargs)
            
            logging.info(params)
            job_params.append(params)
            if self.test and count > 1:
                logging.warning("Breaking after 2 batches while in "
                                "test mode.")
                logging.warning(job_params)
                break

        logging.warning("Batch preparation completed, "
                        f"with {len(job_params)} batches")
        return job_params

    def combine(self, job_params):
        '''Touch the checkpoint'''
        self.output().touch()
