'''
Crunchbase data collection and processing
==================================

Luigi routine to collect Crunchbase data exports and load the data into MySQL.

This task picks up the missed org_parents table and combines this with organizations.
'''

import logging
import luigi

from crunchbase_parent_id_collect_task import ParentIdCollectTask
from nesta.packages.health_data.process_mesh import retrieve_mesh_terms, format_mesh_terms
from nesta.packages.misc_utils.batches import split_batches
from nesta.production.luigihacks import misctools
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.orms.crunchbase_orm import Organization
from nesta.production.orms.orm_utils import get_mysql_engine, db_session


class DescriptionMeshTask(luigi.Task):
    ''' Collects and combines Mesh terms from S3, Abstracts from MYSQL and projects in
    Elasticsearch.

    Args:
        date (str): Date used to label the outputs
        _routine_id (str): String used to label the AWS task
        db_config_path (str): Path to the MySQL database configuration
    '''
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    test = luigi.BoolParameter()
    db_config_env = luigi.Parameter()
    db_config_path = luigi.Parameter()
    insert_batch_size = luigi.IntParameter(default=500)

    def requires(self):
        '''Collects the configurations and executes the previous task.'''
        yield ParentIdCollectTask(date=self.date,
                                  _routine_id=self._routine_id,
                                  test=self.test,
                                  insert_batch_size=self.insert_batch_size,
                                  db_config_path=self.db_config_path,
                                  db_config_env=self.db_config_env)

    def output(self):
        '''Points to the output database engine'''
        db_config = misctools.get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Crunchbase Mesh <dummy>"  # Note, not a real table
        update_id = "CrunchbaseMeshCollect_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        # database setup
        database = 'dev' if self.test else 'production'
        logging.warning(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)

        # collect mesh terms from S3
        bucket = 'innovation-mapping-general'
        key = 'crunchbase_descriptions/crunchbase_descriptions_mesh.txt'
        mesh_terms = retrieve_mesh_terms(bucket, key)
        mesh_terms = format_mesh_terms(mesh_terms)  # [{'id': ['term1', 'term2']}, ...]
        logging.info(f"File contains {len(mesh_terms)} orgs with mesh terms")

        logging.info("Extracting previously processed orgs")
        with db_session(self.engine) as session:
            all_orgs = session .query(Organization.id, Organization.mesh_terms).all()
        processed_orgs = {org_id for (org_id, mesh_terms) in all_orgs
                          if mesh_terms is not None}
        all_orgs = {org_id for (org_id, _) in all_orgs}
        logging.info(f"{len(all_orgs)} total orgs in database")
        logging.info(f"{len(processed_orgs)} previously processed orgs")

        # reformat for batch insert, removing not found and previously processed terms
        meshed_orgs = [{'id': org_id, 'mesh_terms': ','.join(terms)}
                       for org_id, terms in mesh_terms.items()
                       if org_id in all_orgs and org_id not in processed_orgs]

        logging.info(f"{len(meshed_orgs)} organisations to update in database")

        for count, batch in enumerate(split_batches(meshed_orgs,
                                                    self.insert_batch_size), 1):
            with db_session(self.engine) as session:
                session.bulk_update_mappings(Organization, batch)
            logging.info(f"{count} batch{'es' if count > 1 else ''} written to db")
            if self.test and count > 1:
                logging.info("Breaking after 2 batches while in test mode")
                break

        # mark as done
        logging.warning("Task complete")
        self.output().touch()
