import luigi
from datetime import datetime
import logging

from nesta.production.luigihacks.misctools import find_filepath_from_pathstub
from unsdg_query_task import QueryGroupTask


class RootTask(luigi.WrapperTask):
    ''' A dummy root task, which collects the database configurations and
    executes the central task.

    Args:

    '''
    date = luigi.DateParameter(default=datetime.today())
    db_config_path = luigi.Parameter(default="mysql.config")
    production = luigi.BoolParameter(default=False)

    def requires(self):
        ''' requires
        Collects the database configurations and executes the central task.
        '''

        _routine_id = "{}-{}".format(self.date, self.production)

        logging.getLogger().setLevel(logging.INFO)

        yield QueryGroupTask(
                _routine_id=_routine_id,
                date=self.date,
                db_config_path=self.db_config_path,
                test=(not self.production),
                batchable=find_filepath_from_pathstub("unsdg/unsdg_text_predict"),
                env_files=[
                    find_filepath_from_pathstub("nesta/nesta"),
                    find_filepath_from_pathstub("config/mysqldb.config"),
                    find_filepath_from_pathstub("config/elasticsearch.config"),
                    ],
                job_def="py36_amzn1_image",
                job_name=f"QueryGroupTask-{_routine_id}",
                job_queue="HighPriority",
                region_name="eu-west-2",
                poll_time=10,
                memory=1024,
                max_live_jobs=50,
                )
