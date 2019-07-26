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
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub

from nih_collect_task import CollectTask

class ProcessTask(luigi.WrapperTask):
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
        _routine_id = "{}-{}".format(self.date, self.production)

        yield CollectTask(batchable=find_filepath_from_pathstub("batchables/health_data/nih_collect_data"),
                          env_files=[find_filepath_from_pathstub("nesta"),
                                     find_filepath_from_pathstub("/production/config/mysqldb.config")],
                          job_def="py36_amzn1_image",
                          job_name="CollectTask-%s" % _routine_id,
                          job_queue="HighPriority",
                          region_name="eu-west-2",
                          poll_time=10,
                          test=(not self.production),
                          db_config_path=self.db_config_path,
                          memory=2048,
                          production=self.production,
                          max_live_jobs=50,
                          date=self.date)
