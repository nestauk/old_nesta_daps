"""
CooccuranceTableNeo4jTask
===============
Task for turning a co-occurance table from SQL into a Neo4j graph.
"""

from nesta.core.orms.cordis_orm import Base
from nesta.core.orms.orm_utils import db_session_query, get_mysql_engine
from nesta.core.orms.orm_utils import graph_session
from nesta.core.luigihacks.luigi_logging import set_log_level
from nesta.core.luigihacks.misctools import get_config
from eurito_daps.packages.cordis.cordis_neo4j import extract_name, orm_to_neo4j
from eurito_daps.packages.cordis.cordis_neo4j import prepare_base_entities
from nesta.core.luigihacks.mysqldb import MySqlTarget

from datetime import datetime as dt
import luigi
import logging
import os
from py2neo import Graph


class CooccuranceTableNeo4jTask(luigi.Task):
    test = luigi.BoolParameter(default=True)
    date = luigi.DateParameter(default=dt.now())

    def output(self):
        '''Points to the output database engine where the task is marked as done.
        The luigi_table_updates table exists in test and production databases.
        '''
        db_config = get_config(os.environ["MYSQLDB"], "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Cooc Neo4j <dummy>"  # Note, not a real table
        update_id = "CooccuranceTableNeo4j_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        # Add stuff here
        
        # Confirm the task is finished
        self.output().touch()


class RootTask(luigi.WrapperTask):
    production = luigi.BoolParameter(default=False)
    date = luigi.DateParameter(default=datetime.datetime.today())

    def requires(self):
        test = not self.production
        return CooccuranceTableNeo4jTask(test=test,
                                         date=date)
