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
from nesta.production.orms.mesh_orm import MeshTerms, Base
from nesta.production.orms.nih_orm import Projects
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.luigihacks.misctools import get_config

from nesta.packages.health_data.process_mesh import (retrieve_mesh_terms, 
        format_mesh_terms)

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
        session = db_session(engine)
        
        association_table = Base.metadata.tables['nih_mesh_terms']
        docs_done = {d.project_id for d in session.query(association_table).distinct()}

        mesh_terms = session.query(MeshTerms.id, MeshTerms.term).all()
        mesh_terms = {m.term: m.id for m in mesh_terms}
        
        for key in keys:
            df_mesh = retrieve_mesh_terms(bucket, key)
            doc_terms = format_mesh_terms(df_mesh)
            data = []
            for i, (doc, terms) in enumerate(doc_terms.items()):
                doc_terms = []
                if self.test & (i > 2):
                    continue
                if doc in docs_done:
                    continue
                else:
                    for term in terms:
                        if term in mesh_terms:
                            term_id = mesh_terms[term]
                        else:
                            objs = insert_data(self.db_config_env, 'mysqldb', db,
                                    Base, MeshTerms, [{'term': term}])
                            term_id = objs[0].id
                        doc_terms.append({'project_id': doc, 'mesh_term_id': term_id})
                    insert_data(self.db_config_env, 'mysqldb', db,
                        Base, association_table, doc_terms)

