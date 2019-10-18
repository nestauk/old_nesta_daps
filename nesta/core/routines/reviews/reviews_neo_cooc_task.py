"""
CooccuranceTableNeo4jTask
===============
Task for turning a co-occurance table from SQL into a Neo4j graph.
"""

from nesta.core.orms.esco_orm import Skills, SkillCooccurrences
from nesta.core.orms.sql2neo import table_to_neo4j
from nesta.core.orms.orm_utils import db_session_query, get_mysql_engine
from nesta.core.orms.orm_utils import graph_session
from nesta.core.luigihacks.luigi_logging import set_log_level
from nesta.core.luigihacks.misctools import get_config
from nesta.core.luigihacks.mysqldb import MySqlTarget

from datetime import datetime as dt
import luigi
import logging
import os


class CooccurrenceTableNeo4jTask(luigi.Task):
    test = luigi.BoolParameter(default=True)
    date = luigi.DateParameter(default=dt.now())
    graph_url = luigi.Parameter()

    def output(self):
        '''Points to the output database engine where the task is marked as done.
        The luigi_table_updates table exists in test and production databases.
        '''
        db_config = get_config(os.environ["MYSQLDB"], "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Cooc Neo4j <dummy>"  # Note, not a real table
        update_id = "CooccurrenceTableNeo4j_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):

        limit = 100 if self.test else None

        # Set up the sql database connection
        database = 'dev' if self.test else 'production'
        logging.warning(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)

        with graph_session(self.graph_url) as tx:

            # Drop all neo4j data in advance
            # (WARNING: this is a hack in lieu of proper db staging/versioning)
            logging.info('Dropping all previous data')
            tx.graph.delete_all()
            for constraint in tx.run('CALL db.constraints'):
                logging.info(f'Dropping constraint {constraint[0]}')
                tx.run(f'DROP {constraint[0]}')

            # Generate nodes from the Skills table
            n = table_to_neo4j(engine=self.engine, transaction=tx,
                               table=Skills,
                               is_node=True, node_label='skill')

            logging.info(f'Created {n} nodes')

            # Generate relationships from the SkillCooccurrences table
            n = table_to_neo4j(session=self.engine, transaction=tx,
                               table=SkillCooccurrences, limit=limit,
                               is_node=False, rel_type='HAS_SAME_OCCUPATIONS')

            logging.info(f'Created {n} links')

        # Confirm the task is finished
        logging.warning("Task complete")
        self.output().touch()


class RootTask(luigi.WrapperTask):
    production = luigi.BoolParameter(default=False)
    date = luigi.DateParameter(default=dt.now())
    graph_url = luigi.Parameter(default="")

    def requires(self):
        test = not self.production
        return CooccurrenceTableNeo4jTask(test=test,
                                          date=self.date,
                                          graph_url=self.graph_url)
