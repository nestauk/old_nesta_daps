from nesta.core.luigihacks.autobatch import AutoBatchTask
from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.luigihacks.misctools import get_config
from nesta.core.orms.orm_utils import db_session
from nesta.core.orms.orm_utils import get_mysql_engine
from nesta.core.orms.cordis_orm import Project
from nesta.packages.misc_utils.batches import split_batches
from nesta.packages.misc_utils.batches import put_s3_batch
from nesta.packages.cordis.cordis_api import get_framework_ids
import luigi

S3BUCKET = "nesta-production-intermediate"


class CordisCollectTask(AutoBatchTask):
    process_batch_size = luigi.IntParameter(default=5000)
    intermediate_bucket = luigi.Parameter(default=S3BUCKET)

    def output(self):
        '''Points to the output database engine'''
        db_config = get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "CordisCollect <dummy>"  # not a real table
        update_id = "CordisCollect_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def prepare(self):
        if self.test:
            self.process_batch_size = 1000
        # MySQL setup
        database = 'dev' if self.test else 'production'
        engine = get_mysql_engine(self.db_config_env,
                                  'mysqldb', database)

        # Get all possible ids
        nrows = 1000 if self.test else None
        all_rcn = set(get_framework_ids('fp7', nrows=nrows) +
                      get_framework_ids('h2020', nrows=nrows))
        # Subtract off all done ids
        with db_session(engine) as session:
            result = session.query(Project).all()
            done_rcn = {r[0] for r in result}
        all_rcn = all_rcn - done_rcn

        # Generate the job params
        batches = split_batches(all_rcn, self.process_batch_size)
        job_params = [{"batch_file": put_s3_batch(batch,
                                                  self.intermediate_bucket,
                                                  self.routine_id),
                       "config": 'mysqldb.config',
                       "db_name": database,
                       "bucket": self.intermediate_bucket,
                       "done": False,
                       'test': self.test}
                      for count, batch in enumerate(batches, 1)]
        return job_params

    def combine(self, job_params):
        self.output().touch()
