'''
ESCO skills' co-occurrences (dummy routine)
====================================
Luigi routine to calculate skills' co-occurrences and write them to a new table
'''
from datetime import datetime
import luigi
import logging
import os

from nesta.core.orms.orm_utils import get_mysql_engine, db_session
from nesta.core.luigihacks import misctools
from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p
from nesta.core.orms.esco_orm import Base, SkillCooccurrences

class RootTask(luigi.WrapperTask):
    '''The root task, which collects the supplied parameters and calls the ESCOSkillsCooccurrences.

    Args:
        date (datetime): Date used to label the outputs
        name (str): name to search for in the swapi
        production (bool): test mode or production mode
    '''
    date = luigi.DateParameter(default=datetime.today())
    production = luigi.BoolParameter(default=False)
    name = luigi.Parameter(default=f"ESCO_cooccurrences_dummy_task-{date}-{not production}")
    db_config_path = luigi.Parameter(default="mysqldb.config")

    def requires(self):
        '''Call the task to run before this in the pipeline.'''

        logging.getLogger().setLevel(logging.INFO)
        return ESCOSkillsCooccurrences(date=self.date,
                                       name=self.name,
                                       test=not self.production,
                                       db_config_env='MYSQLDB',
                                       db_config_path=self.db_config_path,
                               )

class ESCOSkillsCooccurrences(luigi.Task):
    '''Calculate skills cooccurrences in MySQL.
    Args:
        date (datetime): Datetime used to label the outputs
        name (str): String used to label the AWS task
        db_config_env (str): environmental variable pointing to the db config file
        db_config_path (str): The output database configuration
    '''
    date = luigi.DateParameter()
    name = luigi.Parameter()
    test = luigi.BoolParameter(default=True)
    db_config_env = luigi.Parameter()
    db_config_path = luigi.Parameter()

    def output(self):
        '''Points to the output database engine'''
        db_config = misctools.get_config(os.environ[self.db_config_env], "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "ESCO <dummy>"  # Note, not a real table
        update_id = "ESCOSkillsCooccurrences_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):

        logging.info(f"Starting the ESCOSkillsCooccurrences task...")

        # database setup
        database = 'dev' if self.test else 'production'
        logging.warning(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)

        # drop the existing table
        Base.metadata.drop_all(bind=self.engine, tables=[SkillCooccurrences.__table__])

        # create new table
        Base.metadata.create_all(bind=self.engine, tables=[SkillCooccurrences.__table__])

        # run the co-occurrence calculation query
        filepath = f3p('esco_skills_cooc.sql')
        query = open(filepath).read()
        self.engine.execute(query)

        # check that the table has been populated
        query_check = 'SELECT COUNT(*) FROM esco_skill_cooccurrences;'
        result = self.engine.execute(query_check)
        logging.info(f"{result} connections between skills were detected")

        # mark as done
        logging.warning("Task complete")
        self.output().touch()
