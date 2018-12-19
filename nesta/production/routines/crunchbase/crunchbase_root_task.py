'''
Crunchbase data collection and processing
==================================

Luigi routine to collect Crunchbase data exports and load the data into MySQL.
'''

import luigi
import datetime
import logging

from crunchbase_collect_task import NonOrgCollectTask
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub


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
    production = luigi.BoolParameter(default=False)
    insert_batch_size = luigi.IntParameter(default=500)

    def requires(self):
        '''Collects the database configurations and executes the central task.'''
        _routine_id = "{}-{}".format(self.date, self.production)

        logging.getLogger().setLevel(logging.INFO)
        yield NonOrgCollectTask(date=self.date,
                                _routine_id=_routine_id,
                                test=not self.production,
                                db_config_path=find_filepath_from_pathstub("mysqldb.config"),
                                insert_batch_size=self.insert_batch_size,
                                batchable=find_filepath_from_pathstub("batchables/crunchbase/crunchbase_collect"),
                                env_files=[find_filepath_from_pathstub("nesta/nesta/"),
                                           find_filepath_from_pathstub("config/mysqldb.config"),
                                           find_filepath_from_pathstub("config/crunchbase.config")],
                                job_def="py36_amzn1_image",
                                job_name=f"CrunchBaseNonOrgCollectTask-{_routine_id}",
                                job_queue="HighPriority",
                                region_name="eu-west-2",
                                poll_time=10,
                                memory=2048,
                                max_live_jobs=20)
