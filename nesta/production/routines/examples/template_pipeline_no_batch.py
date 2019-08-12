"""
A Task only pipeline
====================
An example of building a pipeline with just a wrapper task and a regular task.
***Update this!***
"""

from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.luigihacks.misctools import get_config

import luigi
import datetime
import os
import logging


class RootTask(luigi.WrapperTask):
    """Collect the supplied parameters and call the previous task.

    Args:
        date (datetime): Date used to label the completed task
        production (bool): enable test (False) or production mode (True)
    """
    date = luigi.DateParameter(default=datetime.datetime.today())
    production = luigi.BoolParameter(default=False)

    def requires(self):
        """Call the previous task in the pipeline."""

        logging.getLogger().setLevel(logging.INFO)
        return MyTaskWhichNeedsAName(date=self.date,
                                     test=not self.production)


class MyTaskWhichNeedsAName(luigi.Task):
    """A task...
    Normally put this in a separate file and import it.

    Args:
        date (datetime): Date used to label the completed task
        test (bool): If True pipeline is running in test mode
    """
    date = luigi.DateParameter(default=datetime.datetime.today())
    test = luigi.BoolParameter()

    # def requires(self):  # delete if this is the first task in the pipeline
    #     yield PreviousTask(#  just pass on parameters the previous tasks need:
    #                        _routine_id=self._routine_id,
    #                        test=self.test,
    #                        insert_batch_size=self.insert_batch_size,
    #                        db_config_env='MYSQLDB')

    def output(self):
        """Points to the output database engine where the task is marked as done."""
        db_config = get_config(os.environ["MYSQLDB"], "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Example <dummy>"  # Note, not a real table
        update_id = "MyTaskWhichNeedsAName_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        results = list()  # get some data here
        for count, result in enumerate(results, 1):
            # perhaps process the data in some way like this

            if count > 1 and self.test:
                logging.info("Breaking after 2 results in test mode")
                break

        # mark as done
        logging.info("Task complete")
        # if running locally, consider using:
        # raise NotImplementedError
        # while testing to prevent the local scheduler from marking the task as done
        self.output().touch()
