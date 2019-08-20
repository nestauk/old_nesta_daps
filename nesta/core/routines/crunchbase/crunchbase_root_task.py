'''
Crunchbase data collection and processing
==================================

Luigi routine to collect Crunchbase data exports and load the data into MySQL.
'''

import luigi
import datetime
import logging

from crunchbase_elasticsearch_task import CrunchbaseSql2EsTask
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p


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
    drop_and_recreate = luigi.BoolParameter(default=False)
    production = luigi.BoolParameter(default=False)
    insert_batch_size = luigi.IntParameter(default=500)

    def requires(self):
        '''Collects the database configurations and executes the central task.'''
        _routine_id = "{}-{}".format(self.date, self.production)

        logging.getLogger().setLevel(logging.INFO)
        yield CrunchbaseSql2EsTask(date=self.date,
                                   _routine_id=_routine_id,
                                   test=not self.production,
                                   drop_and_recreate=self.drop_and_recreate,
                                   db_config_env="MYSQLDB",
                                   insert_batch_size=self.insert_batch_size,
                                   process_batch_size=50000,
                                   intermediate_bucket='nesta-production-intermediate',
                                   batchable=f3p("batchables/crunchbase/crunchbase_elasticsearch"),
                                   env_files=[f3p("nesta/nesta/"),
                                              f3p("config/mysqldb.config"),
                                              f3p("schema_transformations/crunchbase_organisation_members.json"),
                                              f3p("config/elasticsearch.config")],
                                   job_def="py36_amzn1_image",
                                   job_name=f"CrunchBaseElasticsearchTask-{_routine_id}",
                                   job_queue="HighPriority",
                                   region_name="eu-west-2",
                                   poll_time=10,
                                   memory=2048,
                                   max_live_jobs=100)
