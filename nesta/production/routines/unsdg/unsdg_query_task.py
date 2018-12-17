import boto3
import datetime
from elasticsearch import Elasticsearch
import logging
import luigi
import re

from nesta.production.luigihacks import autobatch, misctools
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub


class QueryGroupTask(autobatch.AutoBatchTask):
    ''' Queries Elasticsearch for entries which do not have up-to-date UNSDG
    labels, groups their ids, and writes them to files in S3.

    Args:
        date (str): Date used to label the outputs
        _routine_id (str): String used to label the AWS task
    '''

    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()

    def requires(self):
        pass

    def output(self):
        pass

    @staticmethod
    def get_model_date(model_bucket):
        pass

    @staticmethod
    def all_unlabelled(es_client, model_date):
        ''' all_unlabelled

        Scans through all documents labelled with UNSDGs by a previous model.

        Args:
            model_date (str): date of the current model

        Returns:
            (generator): yields a document at a time
        '''
        #TODO
        query = {
                "query": {
                    "bool": {
                    "filter": [
                        {"range": {"unsdg_model_date": {"gte": model_date}}}
                        ]
                    }
                }
            }
        return scan(es_client, query, index=INDEX, doc_type=DOC_TYPE)

    @staticmethod
    def chunk_ids_to_s3(query_result, bucket, key_prefix, n=10000):
        ''' chunk_ids
        Gets ids from all documents returned by a query and chunks them into
        groups of size n.

        Args:
            query_result (:obj:`elasticsearch.helpers.scan`): generator of docs
            bucket (str): s3 bucket name
            key_prefix (str): s3 key prefix for naming files
            n (int): size of id groups
        '''

        s3_client = boto3.client('s3')

        ids = (str(q['_source']['_id']) for q in query_result)
        chunks = grouper(ids, n , '')

        for i, chunk in enumerate(chunks):
            id_string = '\n'.join(chunk).strip()
            key = key_prefix.format(i)
            response = client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=id_string
                    )
            yield key

    def prepare(self):
        
        # elasticsearch setup
        es_mode = 'rwjf_prod' if not self.test else 'rwjf_dev'
        es_config = misctools.get_config('elasticsearch.config', es_mode)
        es = Elasticsearch(es_config['external_host'], port=es_config['port'])

        # s3 setup
        id_bucket = 'nesta-production-intermediate'
        id_key_prefix = 'unsdg_prediction/ids_{}.txt'
        model_bucket = 'nesta-sdg-classifier'
        model_key_prefix = 'models'

        model_date = "2018-01-01"
        # model_date = get_model_date(model_bucket, model_key_prefix)
        
        query_results = all_unlabelled(es, model_date)

        chunk_ids_to_s3(query_results, id_bucket, id_key_prefix)

