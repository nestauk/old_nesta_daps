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

from _nih_process_task import ProcessTask

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
        logging.getLogger().setLevel(logging.INFO)
        yield ProcessTask(date=self.date,
                          db_config_path=self.db_config_path,
                          production=self.production
                          batchable="home/ec2user/nesta/nesta/production/"
                                    "batchables/health_data/world_reporter_process/"
                          env_files=["home/ec2user/nesta/nesta/production/schemas/tier1/schema_transformations/nih.json",
                                     "home/ec2user/nesta/nesta/production/config/mysqldb.config",
                                     "home/ec2user/nesta/nesta/production/config/elasticsearch.config",
                                     "home/ec2user/nesta/nesta/production/health_data/",
                                     "home/ec2user/nesta/nesta/production/orms/",
                                     "home/ec2user/nesta/nesta/production/decorators/",
                          # job_def="py36_amzn1_image",
                          # job_name="GroupDetails-%s" % _routine_id,
                          # job_queue="HighPriority",
                          # region_name="eu-west-2",
                          # poll_time=10,
                          )
