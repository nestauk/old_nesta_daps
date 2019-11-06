"""
H2020 and FP7 Data Collection
=============================

Collection of H2020 and FP7 projects, organisations, publications and topics
from the unofficial API.
"""

from nesta.core.luigihacks.autobatch import AutoBatchTask
from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.luigihacks.misctools import get_config
from nesta.core.luigihacks.luigi_logging import set_log_level
from nesta.core.orms.orm_utils import db_session
from nesta.core.orms.orm_utils import get_mysql_engine
from nesta.core.orms.cordis_orm import Base
from nesta.core.orms.cordis_orm import Project
from nesta.packages.misc_utils.batches import split_batches
from nesta.packages.misc_utils.batches import put_s3_batch
from nesta.packages.cordis.cordis_api import get_framework_ids
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p

import luigi
from datetime import datetime as dt

S3BUCKET = "nesta-production-intermediate"


class CordisCollectTask(AutoBatchTask):
    process_batch_size = luigi.IntParameter(default=500)
    intermediate_bucket = luigi.Parameter(default=S3BUCKET)
    db_config_path = luigi.Parameter(default=f3p('config/mysqldb.config'))
    db_config_env = luigi.Parameter(default='MYSQLDB')
    routine_id = luigi.Parameter()

    def output(self):
        '''Points to the output database engine'''
        db_conf = get_config(self.db_config_path, "mysqldb")
        db_conf["database"] = 'dev' if self.test else 'production'
        db_conf["table"] = "CordisCollect <dummy>"  # not a real table
        update_id = self.job_name
        return MySqlTarget(update_id=update_id, **db_conf)

    def prepare(self):
        if self.test:
            self.process_batch_size = 100
        # MySQL setup
        database = 'dev' if self.test else 'production'
        engine = get_mysql_engine(self.db_config_env,
                                  'mysqldb', database)

        # Subtract off all done ids
        Base.metadata.create_all(engine)
        with db_session(engine) as session:
            result = session.query(Project.rcn).all()
            done_rcn = {r[0] for r in result}

        # Get all possible ids (or "RCN" in Cordis-speak)
        nrows = 1000 if self.test else None
        all_rcn = set(get_framework_ids('fp7', nrows=nrows) +
                      get_framework_ids('h2020', nrows=nrows))
        all_rcn = all_rcn - done_rcn

        # Generate the job params
        batches = split_batches(all_rcn, self.process_batch_size)
        params = [{"batch_file": put_s3_batch(batch,
                                              self.intermediate_bucket,
                                              self.routine_id),
                   "config": 'mysqldb.config',
                   "db_name": database,
                   "bucket": self.intermediate_bucket,
                   "outinfo": 'dummy',
                   "done": False,
                   'test': self.test}
                  for batch in batches]
        return params

    def combine(self, job_params):
        self.output().touch()


class RootTask(luigi.WrapperTask):
    production = luigi.BoolParameter(default=False)
    date = luigi.DateParameter(default=dt.now())
    set_log_level(True)

    def requires(self):
        batchable = f3p("batchables/cordis/cordis_api")
        env_files = [f3p("nesta"), f3p("config/mysqldb.config")]
        routine_id = f'Cordis-{self.date}-{self.production}'
        return CordisCollectTask(routine_id=routine_id,
                                 test=not self.production,
                                 batchable=batchable,
                                 env_files=env_files,
                                 job_def="py36_amzn1_image",
                                 job_name=f"Collect-{routine_id}",
                                 job_queue="HighPriority",
                                 region_name="eu-west-2",
                                 poll_time=10,
                                 memory=2048,
                                 max_live_jobs=20)
