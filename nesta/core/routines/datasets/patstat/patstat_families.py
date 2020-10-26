"""
Preprocess PATSTAT data
=======================

Select all, and the EU subset of, PATSTAT by document family id.
This SQL operation significantly speeds up transfer to ES.
Note that the "raw" data collection task for PATSTAT sits outside of
this codebase (see :obj:`nestauk/pypatstat`) since

a) PATSTAT is enormous, so we tend to take a manual snapshot, delete and then collect.
b) pypatstat generates a database on-the-fly, to maximise open utility of the tool.
"""

import luigi
import logging
from nesta.packages.patstat.fetch_appln import extract_data
from nesta.packages.misc_utils.batches import split_batches
from nesta.core.orms.patstat_orm import ApplnFamilyEU, ApplnFamilyAll, Base
from nesta.core.luigihacks.mysqldb import make_mysql_target
from nesta.core.luigihacks.luigi_logging import set_log_level
from nesta.core.orms.orm_utils import insert_data
from datetime import datetime as dt
import os


class PatstatFamilyTask(luigi.Task):
    date = luigi.DateParameter(default=dt.now())
    test = luigi.BoolParameter(default=True)

    def output(self):
        return make_mysql_target(self)

    def run(self):
        """Create two seperate tables of PATSTAT families, 
        one for applications featuring an EU country (EURITO legacy table)
        and one for all PATSTAT data.
        """
        limit = 1000 if self.test else None
        database = 'dev' if self.test else 'production'
        for (rgn, _class) in (('eu', ApplnFamilyEU),
                              ('all', ApplnFamilyAll)):
            data = extract_data(limit=limit, region=rgn)
            logging.info(f'Got {len(data)} rows')
            for chunk in split_batches(data, 10000):
                logging.info(f'Inserting chunk of size {len(chunk)}')
                insert_data('MYSQLDB', 'mysqldb', database,
                            Base, _class, chunk, low_memory=True)
        self.output().touch()


class RootTask(luigi.WrapperTask):
    date = luigi.DateParameter(default=dt.now())
    production = luigi.BoolParameter(default=False)

    def requires(self):
        set_log_level(True)
        yield PatstatFamilyTask(date=self.date,
                                test=not self.production)
