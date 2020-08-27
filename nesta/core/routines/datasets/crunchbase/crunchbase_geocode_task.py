'''
Geocoding
=========

Luigi routines to geocode the Organization, FundingRound, Investor, Ipo and People tables.
'''

import logging
import luigi
import os

from nesta.core.routines.datasets.crunchbase.crunchbase_non_org_collect_task import NonOrgCollectTask
from nesta.core.luigihacks.batchgeocode import GeocodeBatchTask
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p
from nesta.core.luigihacks.misctools import get_config
from nesta.core.luigihacks.mysqldb import MySqlTarget


class CBGeocodeBatchTask(GeocodeBatchTask):
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    insert_batch_size = luigi.IntParameter()

    def output(self):
        '''Points to the output database engine'''
        db_config = get_config(os.environ[self.db_config_env], "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Crunchbase <dummy>"  # Note, not a real table
        update_id = self.job_name
        return MySqlTarget(update_id=update_id, **db_config)

    def requires(self):
        yield NonOrgCollectTask(date=self.date,
                                _routine_id=self._routine_id,
                                test=self.test,
                                db_config_path=f3p("mysqldb.config"),
                                insert_batch_size=self.insert_batch_size,
                                batchable=f3p("batchables/crunchbase/crunchbase_collect"),
                                env_files=[f3p("nesta"),
                                           f3p("config/mysqldb.config"),
                                           f3p("config/crunchbase.config")],
                                job_def="py37_amzn2",
                                job_name=f"CrunchBaseNonOrgCollectTask-{self._routine_id}",
                                job_queue="HighPriority",
                                region_name="eu-west-2",
                                poll_time=10,
                                memory=16000,
                                max_live_jobs=20)

    def combine(self, job_params):
        '''Touch the checkpoint'''
        self.output().touch()
