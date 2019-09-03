"""
Working Testing Example
=======================
To determine if the end-to-end test runner is working.

Always runs in test mode.
"""

from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.luigihacks.misctools import get_config

import luigi
import datetime
import os
import logging


class RootTask(luigi.WrapperTask):
    """The root task.

    Args:
        date (datetime): Date used to label the outputs
        production (bool): test mode or production mode
    """
    date = luigi.DateParameter(default=datetime.datetime.today())
    production = luigi.BoolParameter(default=False)  # changed to True below

    def requires(self):
        """Call the task to run before this in the pipeline."""

        logging.getLogger().setLevel(logging.INFO)
        return WorkingTask(date=self.date, test=True)


class WorkingTask(luigi.Task):
    """This task does nothing useful except run.

    Args:
        date (datetime): Date used to label the outputs
        test (bool): run a shorter version of the task if in test mode
    """
    date = luigi.DateParameter(default=datetime.datetime.today())
    test = luigi.BoolParameter()

    def output(self):
        """Points to the output database engine where the task is marked as done.
        """
        db_config = get_config(os.environ["MYSQLDB"], "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "End-to-end Testing"  # Note, not a real table
        update_id = "WorkingTestTask_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        # mark as done
        logging.info("Task complete")
        self.output().touch()
