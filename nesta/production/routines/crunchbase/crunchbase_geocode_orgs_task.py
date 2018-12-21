'''
Crunchbase geocoding
==================================

Luigi routine to geocode the organizations table.
'''

import boto3
import datetime
import logging
import luigi

from crunchbase_non_org_collect_task import NonOrgCollectTask
from nesta.production.luigihacks.batchgeocode import GeocodeBatchTask
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub, get_config
from nesta.production.luigihacks.mysqldb import MySqlTarget


class OrgGeocodeTask(GeocodeBatchTask):

    date = luigi.DateParameter()
    production = luigi.BoolParameter(default=False)
    insert_batch_size = luigi.IntParameter(default=100)

    def output(self):
        '''Points to the output database engine'''
        db_config = get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Crunchbase <dummy>"  # Note, not a real table
        update_id = "CrunchbaseCollectNonOrgData_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def requires(self):
        '''Collects the database configurations and executes the central task.'''
        _routine_id = "{}-{}".format(self.date, self.production)

        logging.getLogger().setLevel(logging.INFO)
        yield NonOrgCollectTask(date=self.date,
                                _routine_id=_routine_id,
                                test=not self.production,
                                db_config_path=find_filepath_from_pathstub("mysqldb.config"),
                                insert_batch_size=self.insert_batch_size,
                                batchable=find_filepath_from_pathstub("batchables/crunchbase/crunchbase_collect"),
                                env_files=[find_filepath_from_pathstub("nesta/nesta/"),
                                           find_filepath_from_pathstub("config/mysqldb.config"),
                                           find_filepath_from_pathstub("config/crunchbase.config")],
                                job_def="py36_amzn1_image",
                                job_name=f"CrunchBaseNonOrgCollectTask-{_routine_id}",
                                job_queue="HighPriority",
                                region_name="eu-west-2",
                                poll_time=10,
                                memory=4096,
                                max_live_jobs=20)

    def combine(self, job_params):
        '''Touch the checkpoint'''
        self.output().touch()
