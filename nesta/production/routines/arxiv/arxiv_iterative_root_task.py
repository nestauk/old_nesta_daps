'''
arXiv data collection and processing
==================================

Luigi routine to collect all data from the arXiv api and load it to MySQL.
'''
import datetime
import logging
import luigi

from arxiv_mag_task import QueryMagTask


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

    def requires(self):
        '''Collects the database configurations
        and executes the central task.'''
        _routine_id = "{}-{}".format(self.date, self.production)

        logging.getLogger().setLevel(logging.INFO)
        yield QueryMagTask(date=self.date,
                           _routine_id=_routine_id,
                           db_config_path=self.db_config_path,
                           db_config_env='MYSQLDB',
                           mag_config_path='mag.config',
                           test=not self.production,
                           articles_from_date=self.articles_from_date)
