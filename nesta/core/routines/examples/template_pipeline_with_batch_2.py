"""
A batch pipeline
================
An example of building a batch pipeline with a wrapper task and batch task prepare and
run steps.
***Update this***
"""
import boto3
import logging
import luigi
import os

from nesta.core.luigihacks import autobatch, misctools
from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.orms.example_orm import Base, MyTable  # example orm
from nesta.core.orms.orm_utils import get_mysql_engine, try_until_allowed, db_session
# from template_pipeline_with_batch_3 import PreviousTask

TEST_BATCHES = 2
# bucket to store done keys from each batch task
S3 = boto3.resource('s3')
_BUCKET = S3.Bucket("nesta-production-intermediate")
DONE_KEYS = set(obj.key for obj in _BUCKET.objects.all())


class MyBatchTaskWhichNeedsAName(autobatch.AutoBatchTask):
    """Collect data to be processed and divide it up into batches.

    Args:
        date (datetime): Datetime used to label the outputs
        _routine_id (str): String used to label the AWS task
        db_config_path: (str) The output database configuration
    """
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    db_config_env = luigi.Parameter()
    intermediate_bucket = luigi.Parameter()
    batch_size = luigi.IntParameter(default=500)  # example parameter
    start_string = luigi.Parameter()  # example parameter

    # def requires(self):  # delete if this is the first task in the pipeline
    #     yield PreviousTask(#  just pass on parameters the previous tasks need:
    #                        _routine_id=self._routine_id,
    #                        test=self.test,
    #                        insert_batch_size=self.insert_batch_size,
    #                        db_config_env='MYSQLDB')

    def output(self):
        """Points to the output database engine where the task is marked as done."""
        db_config_path = os.environ[self.db_config_env]
        db_config = misctools.get_config(db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Example <dummy>"  # Note, not a real table
        update_id = "MyBatchTaskWhichNeedsAName_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def prepare(self):
        """Prepare the batch job parameters"""
        # database setup
        database = 'dev' if self.test else 'production'
        logging.info(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)
        if self.test and database == 'dev':
            logging.warning('Dropping tables')
            Base.metadata.drop_all(self.engine)
        try_until_allowed(Base.metadata.create_all, self.engine)

        with db_session(self.engine) as session:
            if self.test:
                logging.info("Adding test data")
                test_data = []
                for i in range(1000):
                    test_data.append(MyTable(id=i, founded_on='2009-01-01'))
                session.add_all(test_data)
                session.commit()

            logging.info('Retrieving list of records to process')
            total_records = (session
                             .query(MyTable.id)
                             .filter(MyTable.founded_on > '2007-01-01')
                             .count())

        job_params = []  # dictionaries of environmental variables for each batch
        # potential method of generating batches:
        for count, offset in enumerate(range(0, total_records, self.batch_size)):
            key = f"{self.date}_batch_{offset}_{database}"
            done = key in DONE_KEYS
            params = {"config": "mysqldb.config",
                      "db_name": database,
                      "test": self.test,
                      "outinfo": f"s3://{self.intermediate_bucket}/{key}",
                      "done": done,
                      "batch_size": self.batch_size,  # example parameter
                      "start_string": self.start_string,  # example parameter
                      "offset": offset}
            job_params.append(params)
            logging.info(params)

            if self.test and count == TEST_BATCHES:
                logging.info(f"Only {TEST_BATCHES} batches created in test mode")
                break

        return job_params

    def combine(self, job_params):
        """Touch the checkpoint"""
        self.output().touch()
