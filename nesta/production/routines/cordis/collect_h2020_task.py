'''
Cordis H2020 data collection
============================
'''
import luigi
import datetime
import boto3
import logging

from nesta.packages.cordis.get_h2020 import fetch_and_clean
from nesta.packages.cordis.get_h2020 import TOP_URL
from nesta.packages.cordis.get_h2020 import ENTITIES

from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.luigihacks.misctools import get_config
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub

from nesta.production.orms.cordis_h2020_orm import Base
from nesta.production.orms.orm_utils import get_class_by_tablename
from nesta.production.orms.orm_utils import insert_data


class CordisH2020Task(luigi.Task):
    '''Get all Cordis H2020 data'''
    date = luigi.DateParameter()
    test = luigi.BoolParameter()

    def output(self):
        '''Points to the input database target'''
        db_config = get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "production" if not self.test else "dev"
        db_config["table"] = "cordis_h2020 <dummy>"  # NB: not a real table
        return MySqlTarget(update_id=f'CordisH2020Task-{self.date}', **db_config)

    def run(self):
        data = {}
        for entity_name in ENTITIES:
            # Fetch and clean the data
            df = fetch_and_clean(entity_name)
            if entity_name == 'projects':
                data['programmes'] = pop_and_split_programmes(df)
            data[entity_name] = df

        db = 'dev' if self.test else 'production'
        engine = get_mysql_engine("MYSQLDBCONF", 'mysqldb', db)
        for entity_name, df in data.items():
            # Generate the class and table name, then retrieve
            class_name = entity_name[0].upper() + entity_name[1:]
            table_name = f'cordisH2020_{camel_to_snake(class_name)}'
            _class = get_class_by_tablename(table_name)

            # Drop columns which aren't in the schema
            _columns = [col.name for col in _class.__table__.columns]
            drop_columns = [col for col in df.columns if col not in _columns]
            df = df.drop(drop_columns, axis=1)
            
            # Write the data
            _data = df.to_dict(orient='records')
            if self.test and len(data) > 1000:  
                _data = _data[:1000]
            insert_data("MYSQLDBCONF", 'mysqldb', db,
                        Base, _class, _data, low_memory=True)


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
        yield CordisH2020Task(date=self.date, test=not self.production)
