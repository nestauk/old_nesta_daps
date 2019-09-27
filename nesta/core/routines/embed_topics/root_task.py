import luigi
import datetime
import logging
from nesta.core.routines.embed_topics.clustering_task import ClusterVectors
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p


class RootTask(luigi.WrapperTask):
    """A dummy root task, which collects the database configurations
    and executes the central task.
    Args:
        date (datetime): Date used to label the outputs
        db_config_path (str): Path to the MySQL database configuration
        production (bool): Flag indicating whether running in testing
                           mode (False, default), or production mode (True).
        drop_and_recreate (bool): If in test mode, allows dropping the dev index from the ES database.

    """

    date = luigi.DateParameter(default=datetime.date.today())
    production = luigi.BoolParameter(default=False)
    process_batch_size = luigi.IntParameter(default=1000)

    def requires(self):
        """Collects the database configurations
        and executes the central task."""
        _routine_id = "{}-{}".format(self.date, self.production)

        text2vec_task_kwargs = dict(
            date=self.date,
            batchable=("~/nesta/nesta/core/" "batchables/embed_topics/"),
            test=not self.production,
            db_config_env="MYSQLDB",
            process_batch_size=self.process_batch_size,
            intermediate_bucket="nesta-production-intermediate",
            job_def="py36_amzn1_image",
            job_name="text2vectors-%s" % self.date,
            job_queue="HighPriority",
            region_name="eu-west-2",
            env_files=[f3p("nesta/nesta/"), f3p("config/mysqldb.config")],
            routine_id=_routine_id,
            poll_time=10,
            memory=4096,
            max_live_jobs=5,
        )

        cluster_task_kwargs = dict(
            date=self.date, test=not self.production, db_config_env="MYSQLDB"
        )

        logging.getLogger().setLevel(logging.INFO)

        yield ClusterVectors(**cluster_task_kwargs, text2vectors=text2vec_task_kwargs)
