'''
arXiv data collection and processing
==================================

Luigi routine to collect all data from the arXiv api and load it to MySQL.
'''
import datetime
import logging
import luigi

from arxiv_mag_sparql_task import GridTask


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
    articles_from_date = luigi.Parameter(default=None)
    insert_batch_size = luigi.IntParameter(default=500)
    debug = luigi.BoolParameter(default=False)

    def requires(self):
        '''Collects the database configurations
        and executes the central task.'''
        _routine_id = "{}-{}".format(self.date, self.production)

        level = logging.DEBUG if self.debug else logging.INFO
        logging.getLogger().setLevel(level)

        yield GridTask(date=self.date,
                       _routine_id=_routine_id,
                       db_config_path=self.db_config_path,
                       db_config_env='MYSQLDB',
                       mag_config_path='mag.config',
                       test=not self.production,
                       insert_batch_size=self.insert_batch_size,
                       articles_from_date=self.articles_from_date)
