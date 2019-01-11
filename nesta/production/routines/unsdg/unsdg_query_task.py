import boto3
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from itertools import zip_longest
import logging
import luigi

from nesta.production.luigihacks import autobatch, misctools
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub

from nesta.packages.s3_utils.s3_transfer import get_latest 


class QueryGroupTask(autobatch.AutoBatchTask):
    ''' Queries Elasticsearch for entries which do not have up-to-date UNSDG
    labels, groups their ids, and writes them to files in S3.

    Args:
        date (str): Date used to label the outputs
        _routine_id (str): String used to label the AWS task
    '''

    date = luigi.DateParameter()
    db_config_path = luigi.Parameter()
    _routine_id = luigi.Parameter()

    def output(self):
        '''Points to the input database target'''
        update_id = "QueryGroupTask-%s" % self._routine_id
        db_config = misctools.get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "production" if not self.test else "dev"
        db_config["table"] = "NIH sdg group query DUMMY"  # Note, not a real table
        return MySqlTarget(update_id=update_id, **db_config)

    @staticmethod
    def get_model_date(model_bucket, model_key_prefix):
        ''' get_model_date
        Gets date of most recent model from S3.
        '''
        pass

    @staticmethod
    def all_unlabelled(es_client, model_date, index):
        ''' all_unlabelled
        Scans through all documents labelled with UNSDGs by a previous model.

        Args:
            model_date (str): date of the current model

        Returns:
            (generator): yields a document at a time
        '''

        query = {
            "query": {
                "bool": {
                    "should": [
                        { "range" : {
                            "date_unsdg_model" : {
                                "lt" :  model_date
                            }
                        }},
                        { "bool": {
                            "must_not": {
                                "exists": {
                                    "field": "date_unsdg_model"
                                }
                            }
                        }}
                    ]
                }
            }
        }
        return scan(es_client, query, index=index, doc_type='_doc')

    @staticmethod
    def chunk_ids_to_s3(query_result, bucket, key_prefix, n=10000, test=False):
        ''' chunk_ids
        Gets ids from all documents returned by a query and chunks them into
        groups of size n.

        Args:
            query_result (:obj:`elasticsearch.helpers.scan`): generator of docs
            bucket (str): s3 bucket name
            key_prefix (str): s3 key prefix for naming files
            n (int): size of id groups
        '''
        def grouper(iterable, n, fillvalue=None):
            "Collect data into fixed-length chunks or blocks"
            args = [iter(iterable)] * n
            return zip_longest(*args, fillvalue=fillvalue)

        s3_client = boto3.client('s3')

        ids = (str(q['_id']) for q in query_result)
        chunks = grouper(ids, n , '')

        for i, chunk in enumerate(chunks):
            id_string = '\n'.join(chunk).strip()
            key = key_prefix.format(i)
            response = s3_client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=id_string
                    )
            yield key
            if test and i ==2:
                break


    def prepare(self):
        # elasticsearch setup
        es_mode = 'rwjf_prod' if not self.test else 'rwjf_dev'
        es_config = misctools.get_config('elasticsearch.config', es_mode)
        es = Elasticsearch(es_config['external_host'], port=es_config['port'])
        es_index = 'rwjf' if not self.test else 'rwjf_test'

        # s3 setup
        id_bucket = 'nesta-production-intermediate'
        id_key_prefix = 'unsdg_prediction/ids_{}.txt'
        model_bucket = 'nesta-sdg-classifier'
        model_key_prefix = 'models/sdg_classifier'
        dictioanry_key_prefix = 'dictionary/nesta-sdg'
        topic_model_prefix = 'topic_model/nesta-sdg'
        phraser_prefix = 'phraser/nesta-sdg'
        stop_words_prefix = 'stop_words/nesta-sdg'

        latest_model = get_latest(model_bucket, key=model_key_prefix)
        model_date = latest_model.last_modified.strftime('%Y-%m-%dT%H:%M:%S')
        model_key = latest_model.key

        latest_dictionary = get_latest(model_bucket, key=dictionary_key_prefix)
        dictionary_date = latest_dictionary.last_modified.strftime('%Y-%m-%dT%H:%M:%S')
        dictionary_key = latest_dictionary.key

        latest_topic_model = get_latest(model_bucket, key=topic_model_key_prefix)
        topic_model_date = latest_topic_model.last_modified.strftime('%Y-%m-%dT%H:%M:%S')
        topic_model_key = latest_topic_model.key

        latest_phraser = get_latest(model_bucket, key=phraser_key_prefix)
        phraser_date = latest_phraser.last_modified.strftime('%Y-%m-%dT%H:%M:%S')
        phraser_key = latest_phraser.key

        latest_stop_words = get_latest(model_bucket, key=stop_words_key_prefix)
        stop_words_date = latest_stop_words.last_modified.strftime('%Y-%m-%dT%H:%M:%S')
        stop_words_key = latest_stop_words.key

        query_results = self.all_unlabelled(es, model_date, es_mode)
        chunk_key_ids = self.chunk_ids_to_s3(
                query_results,
                id_bucket,
                id_key_prefix,
                test=self.test
                )

        job_params = []
        for chunk_key_id in chunk_key_ids:
            params =  {
                    'id_key': chunk_key_id,
                    'id_bucket': id_bucket,
                    'model_bucket': model_bucket,
                    'model_key': model_key,
                    'dictionary_key': dictionary_key,
                    'topic_model_key': topic_model_key,
                    'phraser_key': phraser_key,
                    'stop_words_key': stop_words_key,
                    'model_date': model_date,
                    'outinfo': es_config,
                    'done': False
                    }
            logging.info(params)
            job_params.append(params)
        return job_params

    def combine(self, job_params):
        self.output().touch()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    process = QueryGroupTask(test=True, batchable='', job_def='', job_name='',
            job_queue='', region_name='', db_config_path='MYSQLDB', date=date,
            _routine_id='')
    process.prepare()