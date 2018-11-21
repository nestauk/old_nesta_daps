'''
arXiv data collection and processing
==================================

Luigi routine to collect all data from the arXiv api and load it to MySQL.
'''

import luigi
import datetime
import logging
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub

from arxiv_collect_task import CollectTask


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
        yield CollectTask(date=self.date,
                          _routine_id=_routine_id,
                          db_config_path=self.db_config_path,
                          test=(not self.production),
                          batchable=find_filepath_from_pathstub("batchables/arxiv/arxiv_collect"),
                          env_files=[find_filepath_from_pathstub("nesta/nesta/"),
                                     find_filepath_from_pathstub("config/mysqldb.config"),
                                     ],
                          job_def="py36_amzn1_image",
                          job_name="ArxivCollectTask-%s" % _routine_id,
                          job_queue="MinimalCpus",
                          region_name="eu-west-2",
                          vcpus=2,
                          poll_time=10,
                          memory=2048,
                          max_live_jobs=20)
