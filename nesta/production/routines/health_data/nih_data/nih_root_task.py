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
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub as f3p
import os

from nih_dedupe_task import DedupeTask


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
    drop_and_recreate = luigi.BoolParameter(default=False)
    ignore_missing = luigi.BoolParameter(default=False)

    def requires(self):
        '''Collects the database configurations
        and executes the central task.'''
        _routine_id = "{}-{}".format(self.date, self.production)

        logging.getLogger().setLevel(logging.INFO)
        yield DedupeTask(date=self.date,
                         ignore_missing=self.ignore_missing,
                         drop_and_recreate=self.drop_and_recreate,
                         routine_id=_routine_id,
                         db_config_path=self.db_config_path,
                         process_batch_size=20000,
                         intermediate_bucket='nesta-production-intermediate',
                         test=(not self.production),
                         batchable=f3p("batchables/health_data/"
                                       "nih_dedupe"),
                         env_files=[f3p("nesta/"),
                                    f3p("config/mysqldb.config"),
                                    f3p("config/elasticsearch.config"),
                                    f3p("nih.json")],
                         job_def="py36_amzn1_image",
                         job_name="NiHDedupeTask-%s" % _routine_id,
                         job_queue="HighPriority",
                         region_name="eu-west-2",
                         poll_time=10,
                         memory=1024,
                         max_live_jobs=50)

        # yield AbstractsMeshTask(date=self.date,
        #                         ignore_missing=self.ignore_missing,
        #                         drop_and_recreate=self.drop_and_recreate,
        #                         _routine_id=_routine_id,
        #                         db_config_path=self.db_config_path,
        #                         test=(not self.production),
        #                         batchable=f3p("batchables/health_data/"
        #                                       "nih_abstract_mesh_data"),
        #                         env_files=[f3p("nesta/"),
        #                                    f3p("config/mysqldb.config"),
        #                                    f3p("config/elasticsearch.config"),
        #                                    f3p("nih.json")],
        #                         job_def="py36_amzn1_image",
        #                         job_name="AbstractsMeshTask-%s" % _routine_id,
        #                         job_queue="HighPriority",
        #                         region_name="eu-west-2",
        #                         poll_time=10,
        #                         memory=1024,
        #                         max_live_jobs=50)
