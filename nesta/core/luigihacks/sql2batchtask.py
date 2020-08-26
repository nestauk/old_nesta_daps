'''
Sql2BatchTask
=============

Task for piping data from MySQL to a batch task in chunks
'''

import logging
import luigi
import os

from nesta.packages.misc_utils.batches import split_batches
from nesta.packages.misc_utils.batches import put_s3_batch
from nesta.core.luigihacks import autobatch
from nesta.core.luigihacks.parameter import SqlAlchemyParameter
from nesta.core.luigihacks.misctools import get_config
from nesta.core.luigihacks.mysqldb import make_mysql_target
from nesta.core.orms.orm_utils import get_mysql_engine
from nesta.core.orms.orm_utils import db_session


class Sql2BatchTask(autobatch.AutoBatchTask):
    '''Launches batch tasks to pipe data from MySQL in batches to
    AWS batches.

    Args:
        date (datetime): Datetime used to label the outputs.
        routine_id (str): String used to label the AWS task.
        intermediate_bucket (str): Name of the S3 bucket where to store the batch ids.
        db_config_env (str): The output database envariable.
        process_batch_size (int): Number of rows to process in a batch.
        id_field (SqlAlchemy selectable attribute): The ID field attribute.
        filter (SqlAlchemy conditional statement): A conditional statement, to be passed
                                                   to query.filter(). This allows for
                                                   subsets of the data to be processed.
        kwargs (dict): Any other job parameters to pass to the batchable.
    '''
    date = luigi.DateParameter()
    routine_id = luigi.Parameter()
    intermediate_bucket = luigi.Parameter()
    db_config_env = luigi.Parameter()
    db_section = luigi.Parameter(default="mysqldb")
    process_batch_size = luigi.IntParameter(default=1000)
    id_field = SqlAlchemyParameter()
    filter = SqlAlchemyParameter(default=None)
    kwargs = luigi.DictParameter(default={})

    def output(self):
        '''Points to the output database engine'''
        return make_mysql_target(self)

    def prepare(self):
        if self.test:
            self.process_batch_size = 1000
            logging.debug("Batch size restricted to "
                          f"{self.process_batch_size} while in test mode")

        # MySQL setup
        database = 'dev' if self.test else 'production'
        engine = get_mysql_engine(self.db_config_env,
                                  self.db_section,
                                  database)

        # Get set of all objects IDs from the database
        with db_session(engine) as session:
            query = session.query(self.id_field)
            if self.filter is not None:
                query = query.filter(self.filter)
            result = query.all()
            all_ids = {r[0] for r in result}
        logging.info(f"Retrieved {len(all_ids)} IDs rom MySQL")

        job_params = []
        for count, batch in enumerate(split_batches(all_ids, self.process_batch_size),1):
            # write batch of ids to s3
            batch_file = put_s3_batch(batch, self.intermediate_bucket,
                                      self.routine_id)
            params = {
                "batch_file": batch_file,
                "config": 'mysqldb.config',
                "db_name": database,
                "bucket": self.intermediate_bucket,
                "done": False,
                'test': self.test,
                'routine_id': self.routine_id
            }
            params.update(self.kwargs)

            logging.info(params)
            job_params.append(params)
            if self.test and count > 1:
                logging.warning("Breaking after 2 batches while in "
                                "test mode.")
                logging.warning(job_params)
                break

        logging.warning("Batch preparation completed, "
                        f"with {len(job_params)} batches")
        return job_params

    def combine(self, job_params):
        '''Touch the checkpoint'''
        self.output().touch()
