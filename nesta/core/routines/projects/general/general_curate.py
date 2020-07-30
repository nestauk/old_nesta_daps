from nesta.core.luigihacks.luigi_logging import set_log_level
from nesta.core.luigihacks.sql2batchtask import Sql2BatchTask
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p
from nesta.core.orms.crunchbase_orm import Organization as CrunchbaseOrg
from nesta.core.luigihacks.misctools import get_config
from nesta.core.luigihacks.mysqldb import MySqlTarget

import luigi
from datetime import datetime as dt
import os

S3_BUCKET='nesta-production-intermediate'
ENV_FILES = ['mysqldb.config', 'nesta']

def kwarg_maker(dataset, routine_id):
    return dict(routine_id=f'{routine_id}_{dataset}',
                env_files=[f3p(f) for f in ENV_FILES],
                batchable=f3p(f'batchables/general/{dataset}/curate'))

class CurateTask(luigi.WrapperTask):
    process_batch_size = luigi.IntParameter(default=1000)
    test = luigi.BoolParameter(default=True)
    date = luigi.DateParameter(default=dt.now())

    def output(self):
        routine_id = f'General-Curate-Root-{self.date}'
        db_config_path = os.environ['MYSQLDB']
        db_config = get_config(db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = f"{routine_id} <dummy>"  # Not a real table
        update_id = f"{self.routine_id}"
        return MySqlTarget(update_id=update_id, **db_config)

    def requires(self):
        set_log_level(True)
        routine_id = f'General-Curate-{self.date}'
        default_kwargs = dict(date=self.date,
                              process_batch_size=self.process_batch_size,
                              job_def='py36_amzn1_image',
                              job_name=routine_id,
                              job_queue='HighPriority',
                              region_name='eu-west-2',
                              poll_time=10,
                              max_live_jobs=50,
                              db_config_env='MYSQLDB',
                              test=self.test,
                              memory=2048,
                              intermediate_bucket=S3_BUCKET)

        params = (('crunchbase', CrunchbaseOrg.id),)
        for dataset, id_field in params:
            yield Sql2BatchTask(id_field=id_field,
                                **kwarg_maker(dataset, routine_id),
                                **default_kwargs)
