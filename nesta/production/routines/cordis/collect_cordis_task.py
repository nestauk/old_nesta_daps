'''
Cordis H2020/FP7 data collection
================================
'''
import luigi
import datetime
import boto3
import logging
import pandas as pd

from nesta.packages.cordis.get_cordis import fetch_and_clean
from nesta.packages.cordis.get_cordis import pop_and_split_programmes
from nesta.packages.cordis.get_cordis import TOP_URL
from nesta.packages.cordis.get_cordis import ENTITIES
from nesta.packages.misc_utils.camel_to_snake import camel_to_snake

from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.luigihacks.misctools import get_config
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub

from nesta.production.orms.cordis_h2020_orm import Base as Base_h2020
from nesta.production.orms.cordis_fp7_orm import Base as Base_fp7
from nesta.production.orms.orm_utils import get_class_by_tablename
from nesta.production.orms.orm_utils import insert_data
from nesta.production.orms.orm_utils import get_mysql_engine

class CordisTask(luigi.Task):
    '''Get all Cordis H2020/FP7 data'''
    date = luigi.DateParameter()
    test = luigi.BoolParameter()

    def output(self):
        '''Points to the input database target'''
        db_config = get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "production" if not self.test else "dev"
        db_config["table"] = "cordis <dummy>"  # NB: not a real table
        return MySqlTarget(update_id=f'CordisTask-{self.date}', **db_config)

    def run(self):
        # Collect
        data = {}
        for i, (fp, entities) in enumerate(ENTITIES.items()):
            data[fp] = {}
            for entity_name in entities:
                logging.info(f'Collecting {fp} {entity_name}')
                # Fetch and clean the data
                df = fetch_and_clean(entity_name, nrows=1000 if self.test else None)
                if fp == 'h2020' and entity_name == 'projects':
                    data[fp]['programmes'] = pop_and_split_programmes(df)
                    data[fp]['project_programmes'] = pd.DataFrame([{'programme_code': code,
                                                                    'project_rcn': row['rcn']}
                                                                   for _, row in df.iterrows()
                                                                   for code in row['programmes']])
                data[fp][entity_name] = df

        # Write
        db = 'dev' if self.test else 'production'
        engine = get_mysql_engine("MYSQLDBCONF", 'mysqldb', db)
        for fp, _data in data.items():
            for entity_name, df in _data.items():
                logging.info(f'Inserting {fp} {entity_name}')
                # Generate the class and table name, then retrieve
                class_name = entity_name[0].upper() + entity_name[1:]
                table_name = f'cordis_{fp}_{camel_to_snake(class_name)}'
                _class = get_class_by_tablename(Base, table_name)

                # Drop columns which aren't in the schema
                _columns = [col.name for col in _class.__table__.columns]
                drop_columns = [col for col in df.columns if col not in _columns]
                df = df.drop(drop_columns, axis=1)

                # Write the data
                _data = [{k: v for k, v in row.items()
                          if not pd.isnull(v)}
                         for row in df.to_dict(orient='records')]
                insert_data("MYSQLDBCONF", 'mysqldb', db,
                            Base, _class, _data, low_memory=True)

        # Touch the output
        self.output().touch()


class CordisRootTask(luigi.WrapperTask):
    '''A dummy root task, which collects the database configurations
    and executes the central task.

    Args:
        date (datetime): Date used to label the outputs
    '''
    date = luigi.DateParameter(default=datetime.date.today())
    production = luigi.BoolParameter(default=False)

    def requires(self):
        '''Collects the database configurations and executes the central task.'''
        logging.getLogger().setLevel(logging.INFO)
        yield CordisTask(date=self.date, test=not self.production)
