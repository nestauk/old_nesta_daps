'''
Gateway to Research data collection
===================================
Luigi routine to collect GtR data, geocode and load to MYSQL.
'''

import datetime
import logging
import luigi

from nesta.production.routines.gtr.gtr_geocode import GtrGeocode


class RootTask(luigi.WrapperTask):
    '''A dummy root task, which collects the database configurations
    and executes the central task.

    Args:
        date (datetime): Date used to label the outputs
    '''
    date = luigi.DateParameter(default=datetime.date.today())
    page_size = luigi.IntParameter(default=10)
    production = luigi.BoolParameter(default=False)

    def requires(self):
        '''Collects the database configurations and executes the central task.'''
        logging.getLogger().setLevel(logging.INFO)
        yield GtrGeocode(date=self.date,
                         db_config_env="MYSQLDB",
                         page_size=self.page_size,
                         job_name=f"GtRGeocode-{self.date}-{self.production}",
                         test=(not self.production))
