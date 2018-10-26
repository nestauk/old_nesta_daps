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

from nih_process_task import ProcessTask

class RootTask(luigi.WrapperTask):
    '''A dummy root task, which collects the database configurations
    and executes the central task.

    Args:
        date (datetime): Date used to label the outputs
        db_config_path (str): Path to the MySQL database configuration
        production (bool): Flag indicating whether running in testing 
                           mode (False, default), or production mode (True).
    '''
    date = luigi.DateParameter(default=datetime.date.today())
    db_config_path = luigi.Parameter(default="mysqldb.config")
    production = luigi.BoolParameter(default=False)

    def requires(self):
        '''Collects the database configurations
        and executes the central task.'''
        _routine_id = "{}-{}".format(self.date, self.production)

        logging.getLogger().setLevel(logging.INFO)
        yield ProcessTask(date=self.date,
                          _routine_id=_routine_id,
                          db_config_path=self.db_config_path,
                          test=(not self.production),
                          batchable=find_filepath_from_pathstub("batchables/health_data/nih_process_data"),
                          env_files=[find_filepath_from_pathstub("nesta/nesta/"),
                                     find_filepath_from_pathstub("config/mysqldb.config"),
                                     find_filepath_from_pathstub("config/elasticsearch.config"),
                                     find_filepath_from_pathstub("nih.json")],
                          job_def="py36_amzn1_image",
                          job_name="ProcessTask-%s" % _routine_id,
                          job_queue="HighPriority",
                          region_name="eu-west-2",
                          poll_time=10,
                          max_live_jobs=50)
