import luigi
import datetime
import logging
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub as f3p

from nesta.production.routines.health_data.nih_data.mesh_join_task import MeshJoinTask

class RootTask(luigi.WrapperTask):

    date = luigi.DateParameter(default=datetime.date.today())
    db_config_env = luigi.Parameter(default='MYSQLDB')
    production = luigi.BoolParameter(default=False)
    
    def requires(self):
        _routine_id  = f"{self.date}-{self.production}"
        
        logging.getLogger().setLevel(logging.INFO)
        yield MeshJoinTask(date=self.date,
                _routine_id=_routine_id,
                db_config_env=self.db_config_env,
                test=(not self.production),
                )
