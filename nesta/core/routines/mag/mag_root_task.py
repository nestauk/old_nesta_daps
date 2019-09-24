"""
MAG data collection and processing
==================================

Luigi pipeline to collect all data from the MAG SPARQL endpoint and load it to SQL.
"""
import luigi
import datetime
import logging

from nesta.core.routines.mag.mag_collect_task import MagCollectSparqlTask


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
    batch_size = luigi.IntParameter(default=5000)
    insert_batch_size = luigi.IntParameter(default=500)
    from_date = luigi.Parameter(default='2000-01-01')
    min_citations = luigi.IntParameter(default=1)

    def requires(self):
        '''Collects the database configurations
        and executes the central task.'''
        _routine_id = "{}-{}".format(self.date, self.production)

        logging.getLogger().setLevel(logging.INFO)
        yield MagCollectSparqlTask(date=self.date,
                                   _routine_id=_routine_id,
                                   db_config_path=self.db_config_path,
                                   db_config_env='MYSQLDB',
                                   test=not self.production,
                                   batch_size=self.batch_size,
                                   insert_batch_size=self.insert_batch_size,
                                   from_date=self.from_date,
                                   min_citations=self.min_citations)
