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

from nih_collect_task import CollectTask
from nesta.production.luigihacks import autobatch


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
        # to Elasticsearch. requires:
            # local config file
            # index and mapping set up in ES

        '''Points to the input database target'''
        update_id = "worldreporter-%s" % self._routine_id
        db_config = misctools.get_config("es.config", "es")
        return ElasticsearchTarget(update_id=update_id,
                                   index=self.index,
                                   doc_type=self.doc_type,
                                   extra_elasticsearch_args={"scheme":"https"},
                                   **db_config)

    def prepare(self):
        pass

    def combine(self):
        self.output().touch()
