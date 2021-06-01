"""
Cord Collect
============

Luigi routine to collect the latest data from CORD
"""
from datetime import datetime as dt
from datetime import timedelta
import luigi
import logging
import pandas as pd

from nesta.packages.cord.cord import cord_data, to_arxiv_format
from nesta.core.orms.arxiv_orm import Article, Base
from nesta.core.orms.orm_utils import get_mysql_engine, db_session, insert_data
from nesta.core.luigihacks import misctools
from nesta.core.luigihacks.mysqldb import MySqlTarget


class CollectCordTask(luigi.Task):
    """Collect CORD articles

    Args:
        date (datetime): Datetime used to label the outputs
        db_config_env (str): environmental variable pointing to the db config file
        db_config_path (str): The output database configuration
        insert_batch_size (int): Number of records to insert into the database at once
        articles_from_date (str): Earliest possible date considered to collect articles.
    """

    date = luigi.DateParameter()
    dont_recollect = luigi.BoolParameter(default=False)
    routine_id = luigi.Parameter()
    test = luigi.BoolParameter(default=True)
    db_config_env = luigi.Parameter()
    db_config_path = luigi.Parameter()
    insert_batch_size = luigi.IntParameter(default=500)

    def output(self):
        """Points to the output database engine"""
        db_config = misctools.get_config(self.db_config_path, "mysqldb")
        db_config["database"] = "dev" if self.test else "production"
        db_config["table"] = "arXlive <dummy>"  # Note, not a real table
        update_id = "CordCollect_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        date = '2020-05-31' if self.test else None
        articles = list(map(to_arxiv_format, cord_data(date=date)))
        db = "dev" if self.test else "production"
        engine = get_mysql_engine(self.db_config_env, "mysqldb", db)
        with db_session(engine) as session:
            insert_data(
                self.db_config_env,
                "mysqldb",
                db,
                Base,
                Article,
                articles,
                low_memory=True,
            )
        # Mark as done
        self.output().touch()


class CordRootTask(luigi.WrapperTask):
    """A dummy root task, which collects the database configurations
    and executes the central task.

    Args:
        date (datetime): Date used to label the outputs
        db_config_path (str): Path to the MySQL database configuration
        production (bool): Flag indicating whether running in testing
                           mode (False, default), or production mode (True).

    """

    date = luigi.DateParameter(default=dt.today())
    db_config_path = luigi.Parameter(default="mysqldb.config")
    production = luigi.BoolParameter(default=False)

    def requires(self):
        routine_id = "{}-{}".format(self.date, self.production)
        logging.getLogger().setLevel(logging.INFO)
        yield CollectCordTask(
            date=self.date,
            routine_id=routine_id,
            test=not self.production,
            db_config_env="MYSQLDB",
            db_config_path=self.db_config_path,
        )
