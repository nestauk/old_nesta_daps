import luigi
import datetime
import logging
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub as f3p

from nesta.production.routines.health_data.nih_data.mesh_join_task import MeshJoinTask

class RootTask(luigi.WrapperTask):

    date = luigi.DateParameter(default=datetime.date.today())
    db_config_path = luigi.Parameter(default='mysqldb.config')
    production = luigi.BoolParameter(default=False)
    
    def requires(self):
        _routine_id  = f"{self.date}-{self.production}"
        
        logging.getLogger().setLevel(logging.INFO)
        yield MeshJoinTask(date=self.date,
                _routine_id=_routine_id,
                db_config_path=self.db_config_path,
                test=(not self.production),
                batchable=f3p("batchables/health_data/nih_mesh_join"),
                env_files=[f3p("nesta/nesta/"),
                           f3p("config/mysqldb.config"),
                           f3p("config/elasticsearch.config"),
                           f3p("nih.json")],
                job_def="py36_amzn1_image",
                job_name="AbstractsMeshTask-%s" % _routine_id,
                job_queue="HighPriority",
                region_name="eu-west-2",
                poll_time=10,
                memory=1024,
                max_live_jobs=50)
