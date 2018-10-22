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
import logging
from luigi.contrib.esindex import ElasticsearchTarget
import pandas as pd
from sqlalchemy.orm import sessionmaker

from nih_collect_task import CollectTask
from nesta.production.luigihacks import autobatch
from nesta.production.orms.orm_utils import get_mysql_engine
from nesta.production.orms.world_reporter_orm import Projects

BATCH_SIZE = 50000

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

    def batch_limits(self, query, batch_size, test=self.test):
        '''Generates first and last ids from a query object, by batch size.'''
        offset = 0
        while True:
            if test and (offset / batch_size > 1):
                break
            rows = query.order_by(Projects.application_id).filter(Projects.application_id > offset).limit(batch_size).all()
            if len(rows) == 0:
                break
            first = rows[0].application_id
            last = rows[-1].application_id
            yield first, last
            offset = last


    def prepare(self):
        '''add logic to check if done and include in params.'''
        engine = get_mysql_engine("MYSQLDB", "mysqldb", "dev")
        Session = sessionmaker(bind=engine)
        session = Session()
        project_query = session.query(Projects)

        batches = self.batch_limits(project_query, BATCH_SIZE)
        job_params = []
        for start, end in batches:
            params = {'start_index': start,
                      'end_index': end,
                      'config': "mysqldb_config",
                      'db': 'production' if not self.test else 'dev'
                      'outinfo': 'esconfig here',
                      'done': 'calc this from es'
                      }
            print(params)
            job_params.append(params)
        return job_params


    def combine(self):
        self.output().touch()
