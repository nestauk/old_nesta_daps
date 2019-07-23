'''
arXiv data collection and processing
====================================

Luigi wrapper to identify the date since the last iterative data collection
'''
from datetime import datetime

import logging
import luigi
from sqlalchemy.sql import text

from nesta.production.routines.arxiv.arxiv_collect_iterative_task import CollectNewTask
from nesta.packages.arxiv.collect_arxiv import extract_last_update_date
from nesta.production.orms.orm_utils import get_mysql_engine, db_session


# prefix for the task name in luigi_table_updates
UPDATE_PREFIX = 'ArxivIterativeCollect'


class DateTask(luigi.WrapperTask):
    '''Collect new data from the arXiv api and dump the
    data in the MySQL server.

    Args:
        date (datetime): Datetime used to label the outputs
        _routine_id (str): String used to label the AWS task
        db_config_env (str): environmental variable pointing to the db config file
        db_config_path (str): The output database configuration
        insert_batch_size (int): number of records to insert into the database at once
        articles_from_date (str): new and updated articles from this date will be
                                  retrieved. Must be in YYYY-MM-DD format
    '''
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    test = luigi.BoolParameter(default=True)
    db_config_path = luigi.Parameter(default="mysqldb.config")
    db_config_env = luigi.Parameter()
    insert_batch_size = luigi.IntParameter(default=500)
    articles_from_date = luigi.Parameter(default=None)

    def requires(self):
        """
        Collects the last date of successful update from the database and launches the
        iterative data collection task.
        """
        # database setup
        database = 'dev' if self.test else 'production'
        logging.warning(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)

        if self.articles_from_date is None:
            logging.info("Extracting latest update date from database")
            query = text("SELECT update_id FROM luigi_table_updates "
                         f"WHERE update_id LIKE '{UPDATE_PREFIX}%'")
            with db_session(self.engine) as session:
                previous_updates = session.execute(query).fetchall()
            previous_updates = [update_id for (update_id, ) in previous_updates]
            try:
                latest_update = extract_last_update_date(UPDATE_PREFIX, previous_updates)
            except ValueError:
                raise ValueError("Date for iterative data collection could not be determined. Set the date manually with --articles-from-date")
            self.articles_from_date = datetime.strftime(latest_update, '%Y-%m-%d')

        logging.info(f"Updating arxiv data from date: {self.articles_from_date}")

        yield CollectNewTask(date=self.date,
                             _routine_id=self._routine_id,
                             db_config_path=self.db_config_path,
                             db_config_env=self.db_config_env,
                             test=self.test,
                             insert_batch_size=self.insert_batch_size,
                             articles_from_date=self.articles_from_date)
