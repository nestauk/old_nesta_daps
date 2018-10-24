'''
NIH data collection and processing
==================================

Luigi routine to collect NIH World RePORTER data
via the World ExPORTER data dump. The routine
transfers the data into the MySQL database before
processing and indexing the data to ElasticSearch.
'''

import luigi
import datetime
from elasticsearch import Elasticsearch
import logging
from luigi.contrib.esindex import MySqlTarget
from sqlalchemy.orm import sessionmaker

from nih_collect_task import CollectTask
from nesta.production.luigihacks import autobatch
from nesta.production.orms.orm_utils import get_elasticsearch_config
from nesta.production.orms.orm_utils import get_mysql_engine
from nesta.production.orms.world_reporter_orm import Projects

BATCH_SIZE = 50000
MYSQLDB_ENV = 'MYSQLDB'
ESCONFIG_ENV = 'ESCONFIG'


class ProcessTask(autobatch.AutoBatchTask):
    '''A dummy root task, which collects the database configurations
    and executes the central task.

    Args:
        date (datetime): Date used to label the outputs
        db_config_path (str): Path to the MySQL database configuration
        production (bool): Flag indicating whether running in testing
                           mode (False, default), or production mode (True).
    '''
    date = luigi.DateParameter(default=datetime.date.today())
    db_config_path = luigi.Parameter()
    production = luigi.BoolParameter(default=False)

    def requires(self):
        '''Collects the database configurations
        and executes the central task.'''
        logging.getLogger().setLevel(logging.INFO)
        yield CollectTask(date=self.date,
                          db_config_path=self.db_config_path,
                          production=self.production)

    def output(self):
        '''Points to the input database target'''
        update_id = "worldreporter-%s" % self._routine_id
        db_config = misctools.get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "production"
        db_config["table"] = "worldreporter"
        return MySqlTarget(update_id=update_id, **db_config)

    def batch_limits(self, query, batch_size):
        '''Generates first and last ids from a query object, by batch size.

        Args:
            query (object): orm query object
            batch_size (int): rows of data in a batch

        Returns:
            first (int), last (int) application_ids
            '''
        offset = 0
        while True:
            if self.test and (offset / batch_size > 1):  # break after 2 batches
                break
            rows = query.order_by(Projects.application_id).filter(Projects.application_id > offset).limit(batch_size).all()
            if len(rows) == 0:
                break
            first = rows[0].application_id
            last = rows[-1].application_id
            yield first, last
            offset = last

    @staticmethod
    def es_exists_check(es, uid):
        '''Checks to see if the document exists within elasticsearch.

        Args:
            es (object): elasticsearch client
            uid (int): document to be checked

        Returns:
            (bool)
        '''
        return es.exists(index='rwjf', doc_type='_doc', id=uid)

    def prepare(self):
        db = 'production' if not self.test else 'dev'
        engine = get_mysql_engine(MYSQLDB_ENV, "mysqldb", db)

        es_index = 'rwjf_prod' if not self.test else 'rwjf_dev'
        es_config = get_elasticsearch_config(ESCONFIG_ENV, es_index)

        es = Elasticsearch(es_config['host'], sniff_on_start=True)

        Session = sessionmaker(bind=engine)
        session = Session()
        project_query = session.query(Projects)

        batches = self.batch_limits(project_query, BATCH_SIZE, test=self.test)
        job_params = []
        for start, end in batches:
            params = {'start_index': start,
                      'end_index': end,
                      'config': "mysqldb_config",
                      'db': db,
                      'outinfo': es_config['host'],
                      'out_port': es_config['port'],
                      'out_index': es_config['index'],
                      'out_type': es_config['type'],
                      'done': self.es_exists_check(es, end, index=es_config['index'],
                                                   doc_type=es_config['type'])
                      }
            print(params)
            job_params.append(params)
        return job_params

    def combine(self):
        self.output().touch()
