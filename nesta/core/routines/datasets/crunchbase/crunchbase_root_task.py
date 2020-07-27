'''
Root task (Crunchbase)
========================

Luigi routine to collect all data from the Crunchbase data dump and load it to MySQL.
'''

import luigi
import datetime
import logging


from nesta.core.routines.datasets.crunchbase.crunchbase_parent_id_collect_task import ParentIdCollectTask
from nesta.core.routines.datasets.crunchbase.crunchbase_geocode_task import OrgGeocodeTask
from nesta.core.orms.crunchbase_orm import Organization
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
    production = luigi.BoolParameter(default=False)
    insert_batch_size = luigi.IntParameter(default=500)
    db_config_path = luigi.Parameter(default=f3p("mysqldb.config"))
    db_config_env = luigi.Parameter(default="MYSQLDB")
    yield ParentIdCollectTask(date=self.date,
                              _routine_id=self._routine_id,
                              test=self.test,
                              insert_batch_size=self.insert_batch_size,
                              db_config_path=self.db_config_path,
                              db_config_env=self.db_config_env)

    yield OrgGeocodeTask(date=self.date,
                         _routine_id=self._routine_id,
                         test=self.test,
                         db_config_env=self.db_config_env,
                         city_col=Organization.city,
                         country_col=Organization.country,
                         location_key_col=Organization.location_id,
                         insert_batch_size=self.insert_batch_size,
                         env_files=[f3p("nesta/nesta/"),
                                    f3p("config/mysqldb.config")],
                         job_def="py36_amzn1_image",
                         job_name=f"CrunchBaseOrgGeocodeTask-{self._routine_id}",
                         job_queue="HighPriority",
                         region_name="eu-west-2",
                         poll_time=10,
                         memory=4096,
                         max_live_jobs=2)
