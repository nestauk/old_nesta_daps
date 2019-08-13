import boto3
import datetime
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import logging
import luigi
import re

from nesta.production.orms.orm_utils import get_mysql_engine, db_session, exists, insert_data
from nesta.production.orms.mesh_orm import MeshTerms
from nesta.production.orms.nih_orm import Projects
from nesta.production.luigihacks.mysqldb import MySqlTarget

from nesta.packages.health_data.process_mesh import retrieve_mesh_terms, format_mesh_terms

class MeshJoinTask(luigi.Task):
    '''Joins MeSH labels stored in S3 to NIH projects in MySQL.

    Args:
        date (str):
        _routine_id (str):
        db_config_path (str):
    '''

    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    db_config_path = luigi.Parameter()
    
    @staticmethod
    def get_abstract_file_keys(bucket, key_prefix):
        s3 = boto3.resource('s3')
        s3bucket = s3.Bucket(bucket)
        return {o.key for o in s3bucket.objects.filter(Prefix=key_prefix)}

    def requires():
        pass

    def output():
        pass

    def run():
        db = 'production' if not self.test else 'dev'

        bucket = 'innovation-mapping-general'
        key_prefix = 'nih_abstracts_processed/mti'
        keys = self.get_abstract_file_keys(bucket, key_prefix)
        for key in keys:
            df_mesh = retrieve_mesh_terms(bucket, key)
            doc_terms = format_mesh_terms(df_mesh)

            for doc, terms in doc_terms.items():
                for term in terms:
                    if not exists(MeshTerms, {'term': term}):
                        insert_data()



