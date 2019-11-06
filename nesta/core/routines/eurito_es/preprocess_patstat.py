"""
Preprocess PATSTAT data
=======================

Select the EU subset of patstat, by doc family id.
This is will significantly speed up transfer to ES.
"""

import luigi
import logging
from nesta.packages.patstat.fetch_appln_eu import extract_data
from nesta.packages.misc_utils.batches import split_batches
from nesta.core.orms.patstat_eu_orm import ApplnFamily, Base
from nesta.core.luigihacks import misctools
from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.luigihacks.luigi_logging import set_log_level
from nesta.core.orms.orm_utils import insert_data
from datetime import datetime as dt
import os

class PreprocessPatstatTask(luigi.Task):
    date = luigi.DateParameter(default=dt.now())
    test = luigi.BoolParameter(default=True)

    def output(self):
        '''Points to the output database engine'''
        db_config_path = os.environ['MYSQLDB']
        db_config = misctools.get_config(db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "EURITO_patstat_pre"  # Note, not a real table
        update_id = "EURITO_patstat_pre_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        data = extract_data(limit=1000 if self.test else None)
        logging.info(f'Got {len(data)} rows')
        database = 'dev' if self.test else 'production'
        for chunk in split_batches(data, 10000):
            logging.info(f'Inserting chunk of size {len(chunk)}')
            insert_data('MYSQLDB', 'mysqldb', database,
                        Base, ApplnFamily, chunk, low_memory=True)
        self.output().touch()

class PatstatPreprocessRootTask(luigi.WrapperTask):
    date = luigi.DateParameter(default=dt.now())
    production = luigi.BoolParameter(default=False)
    
    def requires(self):
        set_log_level(True)
        yield PreprocessPatstatTask(date=self.date,
                                    test=not self.production)
