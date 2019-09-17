import boto3
import datetime
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import logging
import luigi
import re
import os

from nesta.core.orms.orm_utils import (get_mysql_engine, db_session, 
        exists, insert_data)
from nesta.core.orms.mesh_orm import MeshTerms, ProjectMeshTerms, Base
from nesta.core.orms.nih_orm import Projects
from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.luigihacks.misctools import get_config

from nesta.packages.health_data.process_mesh import retrieve_mesh_terms


bucket = 'innovation-mapping-general'
key_prefix = 'nih_abstracts_processed/mti'

class MeshJoinTask(luigi.Task):
    '''Joins MeSH labels stored in S3 to NIH projects in MySQL.

    Args:
        date (str): Date used to label the outputs
        _routine_id (str): String used to label the AWS task
        db_config_env (str): Environment variable for path to MySQL database
            configuration.
    '''

    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    db_config_env = luigi.Parameter()
    test = luigi.BoolParameter()

    @staticmethod
    def format_mesh_terms(df):
        """
        Removes unrequired columns and pivots the mesh terms data into a dictionary.

        Args:
            df (dataframe): mesh terms as returned from retrieve_mesh_terms

        Returns:
            (dict): document_id: list of mesh terms
        """
        logging.info("Formatting mesh terms")
        # remove PRC rows
        df = df.drop(df[df.term == 'PRC'].index, axis=0)

        # remove invalid error rows
        df = df.drop(df[df.doc_id.astype(str).str.contains('ERROR.*ERROR', na=False)].index, axis=0)
        df['term_id'] = df['term_id'].apply(lambda x: int(x[1:]))

        # pivot and remove unrequired columns
        doc_terms = {
            doc_id: {'terms': grouped.term.values, 'ids': grouped.term_id.values}
            for doc_id, grouped in df.groupby("doc_id")}
        return doc_terms
    
    @staticmethod
    def get_abstract_file_keys(bucket, key_prefix):
        s3 = boto3.resource('s3')
        s3bucket = s3.Bucket(bucket)
        return {o.key for o in s3bucket.objects.filter(Prefix=key_prefix)}

    @staticmethod
    def chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    def output(self):
        db_config = get_config(os.environ[self.db_config_env], "mysqldb")
        db_config['database'] = 'dev' if self.test else 'production'
        db_config['table'] = "MeshTerms <dummy>"
        update_id = "NihJoinMeshTerms_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        db = 'production' if not self.test else 'dev'

        keys = self.get_abstract_file_keys(bucket, key_prefix)
        
        engine = get_mysql_engine(self.db_config_env, 'mysqldb', db)
        with db_session(engine) as session:
            
            existing_projects = set()
            projects = session.query(Projects.application_id).distinct()
            for p in projects:
                existing_projects.update({int(p.application_id)})
            
            projects_done = set()
            projects_mesh = session.query(ProjectMeshTerms.project_id).distinct()
            for p in projects_mesh:
                projects_done.update({int(p.project_id)})
            
            mesh_term_ids = {int(m.id) for m in session.query(MeshTerms.id).all()}

        logging.info('Inserting associations')
        
        for key_count, key in enumerate(keys):
            mesh_term_objs = []
            rows = []
            if self.test and (key_count > 2):
                continue
            # collect mesh results from s3 file and groups by project id
            # each project id has set of mesh terms and corresponding term ids
            df_mesh = retrieve_mesh_terms(bucket, key)
            n_terms = df_mesh.shape[0]
            logging.info(f'Found {n_terms} MeSH terms.')
            project_terms = self.format_mesh_terms(df_mesh)
            # go through documents
            for project_count, (project_id, terms) in enumerate(project_terms.items()):
                if self.test and (project_count > 2):
                    continue
                if (project_id in projects_done) or (project_id not in existing_projects):
                    continue

                for term, term_id in zip(terms['terms'], terms['ids']):
                    term_id = int(term_id)
                    # add term to mesh term table if not present
                    if term_id not in mesh_term_ids:
                        mesh_table_objs.append({'id': term_id, 'term': term})
                        mesh_term_ids.update({term_id})
                    # prepare row to be added to project-mesh_term link table
                    rows.append({'project_id': project_id, 'mesh_term_id': term_id})
            # insert all missing mesh labels
            mesh_objs = insert_data(self.db_config_env, 'mysqldb', db, Base, MeshTerms, 
                    mesh_term_objs, low_memory=True)
            # inesrt rows to link table
            chunk_size = 10000
            for i, chunk in enumerate(chunks(rows, chunk_size)):
                start = i * chunk_size
                end = start + chunk_size - 1
                d = datetime.datetime.utcnow().strftime('%H:%M:%S %d-%m-%Y')
                logging.info(f'{d}: Inserting terms {start} to {end}')
                insert_data(self.db_config_env, 'mysqldb', db, Base, ProjectMeshTerms, 
                        chunk, low_memory=True)
        self.output().touch() # populate project-mesh_term link table

