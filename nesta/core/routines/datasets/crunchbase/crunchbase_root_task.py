'''
Root task (Crunchbase)
========================

Luigi routine to collect all data from the Crunchbase data dump and load it to MySQL.
'''

import luigi
import datetime
import logging

from nesta.core.routines.datasets.crunchbase.crunchbase_parent_id_collect_task import ParentIdCollectTask
from nesta.core.routines.datasets.crunchbase.crunchbase_geocode_task import CBGeocodeBatchTask
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p
from nesta.core.orms.crunchbase_orm import Base
from nesta.core.orms.orm_utils import get_class_by_tablename


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
    db_config_path = luigi.Parameter(default=f3p("mysqldb.config"))
    db_config_env = luigi.Parameter(default="MYSQLDB")
    
    def requires(self):
        '''Collects the database configurations and executes the central task.'''
        _routine_id = "{}-{}".format(self.date, self.production)

        logging.getLogger().setLevel(logging.INFO)        
        yield ParentIdCollectTask(date=self.date,
                                  _routine_id=_routine_id,
                                  test=not self.production,
                                  insert_batch_size=self.insert_batch_size,
                                  db_config_path=self.db_config_path,
                                  db_config_env=self.db_config_env)

        geocode_kwargs = dict(date=self.date,
                              _routine_id=_routine_id,
                              test=not self.production,
                              db_config_env="MYSQLDB",
                              insert_batch_size=self.insert_batch_size,
                              env_files=[f3p("nesta"),
                                         f3p("config/mysqldb.config"),
                                         f3p("config/crunchbase.config")],
                              job_def="py37_amzn2",
                              job_queue="HighPriority",
                              region_name="eu-west-2",
                              poll_time=10,
                              memory=4096,
                              max_live_jobs=2)

        for tablename in ['organizations', 'funding_rounds', 'investors', 'people', 'ipos']:
            _class = get_class_by_tablename(Base, f'crunchbase_{tablename}')
            yield CBGeocodeBatchTask(city_col=_class.city,
                                     country_col=_class.country,
                                     location_key_col=_class.location_id,
                                     job_name=f"Crunchbase-{tablename}-{_routine_id}",
                                     **geocode_kwargs)

