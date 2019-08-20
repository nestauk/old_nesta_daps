from nesta.packages.nomis.nomis import process_config
from nesta.packages.nomis.nomis import batch_request
from nesta.packages.nomis.nomis import reformat_nomis_columns

from nesta.core.orms.orm_utils import insert_data
from nesta.core.orms.orm_utils import get_class_by_tablename
from nesta.core.orms.nomis_orm import Base

from nesta.core.luigihacks import misctools
from nesta.core.luigihacks.mysqldb import MySqlTarget

import datetime
import luigi
import logging

MYSQLDB_ENV = 'MYSQLDB'

class NomisTask(luigi.Task):
    config_name = luigi.Parameter()
    production = luigi.BoolParameter(default=False)
    date = luigi.DateParameter()

    def output(self):
        '''Points to the output database engine'''
        db_config = misctools.get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "production" if self.production else "dev"
        db_config["table"] = "nomis (dummy) "
        update_id = (f"{db_config['table']} {self.date} "
                     f"{self.config_name} {self.production}")
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        config, geogs_list, dataset_id, date_format = process_config(self.config_name,
                                                                     test=not self.production)
        for igeo, geographies in enumerate(geogs_list):
            logging.debug(f"Geography number {igeo}")
            done = False
            record_offset = 0
            while not done:
                logging.debug(f"\tOffset of {record_offset}")
                df, done, record_offset = batch_request(config, dataset_id, geographies,
                                                        date_format, max_api_calls=10,
                                                        record_offset=record_offset)
                data = {self.config_name: df}
                tables = reformat_nomis_columns(data)
                for name, table in tables.items():
                    logging.debug(f"\t\tInserting {len(table)} into nomis_{name}...")
                    _class = get_class_by_tablename(Base, f"nomis_{name}")
                    objs = insert_data(MYSQLDB_ENV, "mysqldb",
                                       "production" if self.production else "dev",
                                       Base, _class, table, low_memory=True)
                    logging.debug(f"\t\tInserted {len(objs)}")
                    
        #data = get_nomis_data(self.config_name, test=not self.production)   
        #tables = reformat_nomis_columns({self.config_name:data})

        self.output().touch()


class RootTask(luigi.WrapperTask):
    date = luigi.DateParameter(default=datetime.date.today())
    production = luigi.BoolParameter(default=False)

    def requires(self):
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("botocore").setLevel(logging.WARNING)
        
        #configs = ["claimantcount", "median_wages",
        #           "population_estimate", "employment", 
        #           "workforce_jobs", "businesscount"]
        configs = ["businesscount"]

        #if not self.production:
        #    configs = ["employment"]
        for c in configs:            
            yield NomisTask(config_name=c, production=self.production, date=self.date)
