"""
MAG data collection and processing
==================================

Luigi pipeline to collect all data from the MAG SPARQL endpoint and load it to SQL.
"""
import luigi
import datetime
import logging

from nesta.core.routines.mag.mag_estimate_task import MagEstimateSparqlTask


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
        yield MagEstimateSparqlTask(date=self.date,
                                    _routine_id=_routine_id,
                                    db_config_path=self.db_config_path,
                                    db_config_env='MYSQLDB',
                                    test=(not self.production))
