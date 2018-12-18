import boto3
from elasticsearch import Elasticsearch
import logging
import luigi

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
#         query = {
#                 "query": {
#                     "bool": {
#                     "filter": [
#                         {"range": {"unsdg_model_date": {"gte": model_date}}}
#                         ]
#                     }
#                 }
#             }
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
        es_mode = 'rwjf' if not self.test else 'rwjf_test'
        es_config = misctools.get_config('elasticsearch.config', es_mode)
        es = Elasticsearch(es_config['external_host'], port=es_config['port'])
        es_index = 'rwjf' if not self.test else 'rwjf_test'

        # s3 setup
        id_bucket = 'nesta-production-intermediate'
        id_key_prefix = 'unsdg_prediction/ids_{}.txt'
        model_bucket = 'nesta-sdg-classifier'
        model_key_prefix = 'models'

        model_date = "2018-01-01"
        # model_date = get_model_date(model_bucket, model_key_prefix)
        
        query_results = all_unlabelled(es, model_date, es_mode)

        chunk_key_ids = chunk_ids_to_s3(query_results, id_bucket, id_key_prefix)
        
        job_params = []
        for chunk_key_id in chunk_key_ids:
            params =  {
                    'id_key': id_key_prefix.format(chunk_key_id),
                    'id_bucket': id_bucket,
                    'model_bucket': model_bucket,
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
