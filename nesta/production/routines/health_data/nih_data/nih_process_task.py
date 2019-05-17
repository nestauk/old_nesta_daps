'''
NIH data collection and processing
==================================

Luigi routine to collect NIH World RePORTER data
via the World ExPORTER data dump. The routine
transfers the data into the MySQL database before
processing and indexing the data to ElasticSearch.
'''

import datetime
from elasticsearch import Elasticsearch
import logging
import luigi
from sqlalchemy.orm import sessionmaker

from nesta.production.routines.health_data.nih_data.nih_collect_task import CollectTask
from nesta.production.luigihacks import autobatch, misctools
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.orms.orm_utils import get_mysql_engine
from nesta.production.orms.orm_utils import setup_es
from nesta.production.orms.nih_orm import Projects
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub

BATCH_SIZE = 50000
MYSQLDB_ENV = 'MYSQLDB'


class ProcessTask(autobatch.AutoBatchTask):
    '''A dummy root task, which collects the database configurations
    and executes the central task.

    Args:
        date (str): Date used to label the outputs
        _routine_id (str): String used to label the AWS task
        db_config_path (str): Path to the MySQL database configuration
    '''
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    db_config_path = luigi.Parameter()
    es_mode = luigi.Parameter(default="dev")
    reindex = luigi.BoolParameter(default=False)

    def requires(self):
        '''Collects the database configurations
        and executes the central task.'''
        logging.getLogger().setLevel(logging.INFO)
        yield CollectTask(date=self.date,
                          _routine_id=self._routine_id,
                          db_config_path=self.db_config_path,
                          batchable=find_filepath_from_pathstub("batchables/health_data/nih_collect_data"),
                          env_files=[find_filepath_from_pathstub("nesta/nesta"),
                                     find_filepath_from_pathstub("/production/config/mysqldb.config")],
                          job_def=self.job_def,
                          job_name="CollectTask-%s" % self._routine_id,
                          job_queue=self.job_queue,
                          region_name=self.region_name,
                          poll_time=10,
                          test=self.test,
                          memory=2048,
                          max_live_jobs=2)

    def output(self):
        '''Points to the input database target'''
        update_id = "NihProcessData-%s" % self._routine_id
        db_config = misctools.get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "production" if not self.test else "dev"
        db_config["table"] = "NIH process DUMMY"  # Note, not a real table
        return MySqlTarget(update_id=update_id, **db_config)

    def batch_limits(self, query, batch_size):
        '''
        Determines first and last ids for a batch.

        Args:
            query (object): orm query object
            batch_size (int): rows of data in a batch

        Returns:
            first (int), last (int) application_ids
        '''
        if self.test:
            batch_size = 20

        batches = 0
        last = 0
        while True:
            if self.test and batches > 1:  # break after 2 batches
                break
            rows = query.order_by(Projects.application_id).filter(Projects.application_id > last).limit(batch_size).all()
            if len(rows) == 0:  # all rows have been collected
                break
            first = rows[0].application_id
            last = rows[-1].application_id
            yield first, last
            batches += 1

    def prepare(self):
        # mysql setup
        db = 'production' if not self.test else 'dev'
        engine = get_mysql_engine(MYSQLDB_ENV, "mysqldb", db)
        Session = sessionmaker(bind=engine)
        session = Session()
        project_query = session.query(Projects)

        # elasticsearch setup
        es, es_config = setup_es(self.es_mode, self.test, self.reindex,
                                 dataset='nih',
                                 aliases='health_scanner')


        batches = self.batch_limits(project_query, BATCH_SIZE)
        job_params = []
        for start, end in batches:
            params = {'start_index': start,
                      'end_index': end,
                      'config': "mysqldb.config",
                      'db': db,
                      'outinfo': es_config['host'],
                      'out_port': es_config['port'],
                      'out_index': es_config['index'],
                      'out_type': es_config['type'],
                      'done': es.exists(index=es_config['index'],
                                        doc_type=es_config['type'],
                                        id=end),
                      'aws_auth_region': es_config['region'],
                      'entity_type': 'paper'
                      }
            print(params)
            job_params.append(params)
        return job_params

    def combine(self, job_params):
        self.output().touch()


if __name__ == '__main__':
    process = ProcessTask(batchable='', job_def='', job_name='', job_queue='', region_name='', db_config_path='MYSQLDB')
    process.prepare()
