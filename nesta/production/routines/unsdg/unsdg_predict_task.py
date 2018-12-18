import boto3
from elasticsearch import Elasticsearch
import logging
import luigi

from nesta.production.routines.unsdg.unsdg_query_task import GroupQueryTask
from nesta.production.luigihacks import autobatch, misctools
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub


class UnsdgPredictTask(autobatch.AutoBatchTask):
    ''' Applies a model to Abstracts to predict project UNSDG labels, and
    writes them to Elasticsearch.

    Args:
        
    '''
    
    def requies(self):
        logging.getLogger().setLevel(logging.INFO)
        yield GroupQueryTask(
              _routine_id=self._routine_id,
              db_config_path=self.db_config_path,
              #TODO
              batchable=find_filepath_from_pathstub("batchables/unsdg/..."),
              env_files=[
                  find_filepath_from_pathstub("nesta/nesta/"),
                     find_filepath_from_pathstub("config/mysqldb.config"),
                     find_filepath_from_pathstub("config/elasticsearch.config"),
                     find_filepath_from_pathstub("nih.json")
                     ],
              job_def=self.job_def,
              job_name="GroupQueryTask-%s" % self._routine_id,
              job_queue=self.job_queue,
              region_name=self.region_name,
              poll_time=10,
              test=self.test,
              memory=1024,
              max_live_jobs=2
              )

    def output(self):
        pass

    @staticmethod
    def get_latest_model():
        pass

    @staticmethod
    def get_id_file_keys():
        pass

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

    def combine(self):
        self.output().touch()
