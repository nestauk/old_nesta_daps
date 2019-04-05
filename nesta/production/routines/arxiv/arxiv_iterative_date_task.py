'''
arXiv data collection and processing
====================================

Luigi routine to identify the date since the last iterative data collection
'''

from datetime import datetime, timedelta
import logging
import luigi
from sqlalchemy import func

from arxiv_collect_iterative_task import CollectNewTask
from nesta.production.orms.arxiv_orm import Article
from nesta.production.orms.orm_utils import get_mysql_engine, db_session


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
            with db_session(self.engine) as session:
                latest_update = session.query(func.max(Article.updated)).scalar()
            if latest_update is None:
                raise ValueError("Date for iterative data collection could not be determined")
            latest_update += timedelta(days=1)
            self.articles_from_date = datetime.strftime(latest_update, '%Y-%m-%d')

        yield CollectNewTask(date=self.date,
                             _routine_id=self._routine_id,
                             db_config_path=self.db_config_path,
                             db_config_env=self.db_config_env,
                             test=self.test,
                             articles_from_date=self.articles_from_date)
