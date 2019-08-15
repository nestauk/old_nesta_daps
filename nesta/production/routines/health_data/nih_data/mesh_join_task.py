import boto3
import datetime
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import logging
import luigi
import re
import os

from nesta.production.orms.orm_utils import (get_mysql_engine, db_session, 
        exists, insert_data)
from nesta.production.orms.mesh_orm import MeshTerms, ProjectMeshTerms, Base
from nesta.production.orms.nih_orm import Projects
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.luigihacks.misctools import get_config

from nesta.packages.health_data.process_mesh import retrieve_mesh_terms

class MeshJoinTask(luigi.Task):
    '''Joins MeSH labels stored in S3 to NIH projects in MySQL.

    Args:
        date (str):
        _routine_id (str):
        db_config_env (str):
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

    def output(self):
        db_config = get_config(os.environ[self.db_config_env], "mysqldb")
        db_config['database'] = 'dev' if self.test else 'production'
        db_config['table'] = "MeshTerms <dummy>"
        update_id = "NihJoinMeshTerms_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        db = 'production' if not self.test else 'dev'

        bucket = 'innovation-mapping-general'
        key_prefix = 'nih_abstracts_processed/mti'
        keys = self.get_abstract_file_keys(bucket, key_prefix)
        
        engine = get_mysql_engine(self.db_config_env, 'mysqldb', db)
        with db_session(engine) as session:
        
            docs_done = {d.project_id 
                    for d in session.query(ProjectMeshTerms).distinct()}

            mesh_term_ids = {int(m.id) for m in session.query(MeshTerms.id).all()}
            
            for key in keys:
                df_mesh = retrieve_mesh_terms(bucket, key)
                doc_terms = self.format_mesh_terms(df_mesh)
                data = []
                for i, (doc, t) in enumerate(doc_terms.items()):
                    print(t)
                    doc_terms = []
                    if self.test & (i > 2):
                        continue
                    if doc in docs_done:
                        continue
                    else:
                        for term, term_id in zip(t['terms'], t['ids']):
                            term_id = int(term_id)
                            if term_id not in mesh_term_ids:
                                objs = insert_data(self.db_config_env, 
                                        'mysqldb', db, Base, MeshTerms, 
                                        [{'id': term_id, 'term': term}],
                                        low_memory=True)
                                mesh_term_ids.update({term_id})
                            doc_terms.append({'project_id': doc,
                                'mesh_term_id': term_id})
                        insert_data(self.db_config_env, 'mysqldb', db,
                            Base, ProjectMeshTerms, doc_terms, low_memory=True)

