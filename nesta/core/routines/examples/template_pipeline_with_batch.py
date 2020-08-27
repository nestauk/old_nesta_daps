"""
A batch pipeline
================
An example of building a batch pipeline with a wrapper task and batch task prepare and
run steps.
***Update this***
"""
import datetime
import logging
import luigi

from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p
from nesta.core.routines.examples.template_pipeline_with_batch_2 import MyBatchTaskWhichNeedsAName


class RootTask(luigi.WrapperTask):
    """Collect the supplied parameters and call the previous task.

    Args:
        date (datetime): Date used to label the outputs
        db_config_path (str): Path to the MySQL database configuration
        production (bool): Flag indicating whether running in testing
                           mode (False, default), or production mode (True).
    """
    date = luigi.DateParameter(default=datetime.date.today())
    production = luigi.BoolParameter(default=False)
    batch_size = luigi.IntParameter(default=500)  # example parameter
    start_string = luigi.Parameter(default="cat")  # example parameter

    def requires(self):
        """Collects the database configurations and executes the central task."""
        _routine_id = "{}-{}".format(self.date, self.production)

        logging.getLogger().setLevel(logging.INFO)
        yield MyBatchTaskWhichNeedsAName(date=self.date,
                                         _routine_id=_routine_id,
                                         test=not self.production,
                                         db_config_env="MYSQLDB",
                                         batch_size=self.batch_size,  # example parameter
                                         start_string=self.start_string,  # example parameter
                                         intermediate_bucket="nesta-production-intermediate",
                                         batchable=f3p("batchables/examples/template_batchable"),  # folder name
                                         env_files=[f3p("nesta/nesta/"),
                                                    f3p("config/mysqldb.config")],
                                         job_def="py37_amzn2",
                                         job_name=f"MyBatchTaskWhichNeedsAName-{_routine_id}",
                                         job_queue="LowPriority",
                                         region_name="eu-west-2",
                                         poll_time=10,
                                         memory=2048,
                                         max_live_jobs=10)
