"""
Pipeline to fetch latest companies house database and put into database
"""

import datetime
import logging
import os

import luigi

from nesta.packages.companies_house.collect_ch_data_dump import (
    clean_ch, download_data_dump)
from nesta.production.luigihacks.misctools import get_config
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.orms.companies_house_orm import Base, Company
from nesta.production.orms.orm_utils import insert_data

MYSQLDB_ENV = "MYSQLDB"

class RootTask(luigi.WrapperTask):
    """ Root task

    Args:
        date(`datetime`): Date used to label the outputs and construct data-dump URL
        production (`bool`): Test mode or production mode
    """

    date = luigi.DateParameter(default=datetime.datetime.today())
    production = luigi.BoolParameter(default=False)

    def requires(self):
        """ Call previous task """

        logging.getLogger().setLevel(logging.INFO)

        return CHDataDump(date=self.date, test=not self.production)

class CHDataDump(luigi.Task):
    """ Collects latest data dump and puts them into a db

    Args:
        date(`datetime`): Date used to label the outputs and construct data-dump URL
        test (`bool`): Test mode or production mode
    """
    date = luigi.DateParameter(default=datetime.datetime.today())
    test = luigi.BoolParameter()

    def output(self):
        """ """
        db_config = get_config(os.environ[MYSQLDB_ENV], "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "CompaniesHouse <dummy>"
        update_id = f"CHDataDump_{self.date}"
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        if self.test:
            nrows = 1000
        else:
            nrows = None

        df = download_data_dump(self.date, cache=False, nrows=nrows).pipe(clean_ch)

        # Write data to DB
        objs = insert_data(MYSQLDB_ENV, "mysqldb",
                           "production" if not self.test else "dev",
                           Base, Company, df.to_dict('records'), low_memory=True)

        self.output().touch()


    def requires(self):
        pass
