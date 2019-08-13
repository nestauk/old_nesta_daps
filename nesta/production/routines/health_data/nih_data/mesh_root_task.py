import luigi
import datetime
import logging
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub as f3p

from mesh_join_task import MeshJoinTask

class RootTask(luigi.WrapperTask):

    date = luigi.DateParameter(default=datetime.date.today())
    db_config_path = luigi.Parameter(default='mysqldb.config')
    production = luigi.BoolParameter(default=False)
    
    def requires(self):
        _routine_id  = f"{self.date}-{self.production}"
        
        logging.getLogger().setLevel(logging.INFO)
        yield MeshJoinTask(date=self.date,
                _routine_id=routine_id,
                db_config_path=self.db_config_path,
                test=(not self.production),
                batchable=find_filepath_from_pathstub("batchables/health_data/nih_mesh_join"),
                env_files=[find_filepath_from_pathstub("nesta/nesta/"),
                           find_filepath_from_pathstub("config/mysqldb.config"),
                           find_filepath_from_pathstub("config/elasticsearch.config"),
                           find_filepath_from_pathstub("nih.json")],
                job_def="py36_amzn1_image",
                job_name="AbstractsMeshTask-%s" % _routine_id,
                job_queue="HighPriority",
                region_name="eu-west-2",
                poll_time=10,
                memory=1024,
                max_live_jobs=50)
