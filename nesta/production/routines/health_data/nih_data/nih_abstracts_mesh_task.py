'''
NIH data collection and processing
==================================

Luigi routine to collect NIH World RePORTER data
via the World ExPORTER data dump. The routine
transfers the data into the MySQL database before
processing and indexing the data to ElasticSearch.
'''

import boto3
import datetime
from elasticsearch import Elasticsearch
import logging
import luigi
import re
from sqlalchemy.orm import sessionmaker

from nesta.production.routines.health_data.nih_data.nih_process_task import ProcessTask
from nesta.production.luigihacks import autobatch, misctools
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.orms.orm_utils import get_mysql_engine
from nesta.production.orms.nih_orm import Abstracts
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub


class AbstractsMeshTask(autobatch.AutoBatchTask):
    ''' Collects and combines Mesh terms from S3, Abstracts from MYSQL and projects in
    Elasticsearch.

    Args:
        date (str): Date used to label the outputs
        _routine_id (str): String used to label the AWS task
        db_config_path (str): Path to the MySQL database configuration
    '''
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    db_config_path = luigi.Parameter()

    def requires(self):
        '''Collects the database configurations
        and executes the central task.'''
        logging.getLogger().setLevel(logging.INFO)
        yield ProcessTask(date=self.date,
                          _routine_id=self._routine_id,
                          db_config_path=self.db_config_path,
                          batchable=find_filepath_from_pathstub("batchables/health_data/nih_process_data"),
                          env_files=[find_filepath_from_pathstub("nesta/nesta/"),
                                     find_filepath_from_pathstub("config/mysqldb.config"),
                                     find_filepath_from_pathstub("config/elasticsearch.config"),
                                     find_filepath_from_pathstub("nih.json")],
                          job_def=self.job_def,
                          job_name="ProcessTask-%s" % self._routine_id,
                          job_queue=self.job_queue,
                          region_name=self.region_name,
                          poll_time=10,
                          test=self.test,
                          memory=2048,
                          max_live_jobs=2)

    def output(self):
        '''Points to the input database target'''
        update_id = "NihProcessData-%s" % self._routine_id
        db_config = misctools.get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "production" if not self.test else "dev"
        db_config["table"] = "NIH abstracts mesh DUMMY"  # Note, not a real table
        return MySqlTarget(update_id=update_id, **db_config)

    @staticmethod
    def get_abstract_file_keys(bucket, key_prefix):
        s3 = boto3.resource('s3')
        s3bucket = s3.Bucket(bucket)
        return {b.key for b in s3bucket.objects.filter(Prefix=key_prefix)}

    @staticmethod
    def done_check(es_client, index, doc_type, key):
        pattern = r'(\d+)-(\d+)\.txt$'
        match = re.search(pattern, key)
        # start, end = match.groups()
        for idx in match.groups():
            res = es_client.get(index='rwjf_dev', doc_type='_doc', id=idx)
            if res['_source'].get('MESH FIELD NAME') is None:
                return False
        return True



        # TODO: update the ES mapping for the mesh terms and then determine how to query to
        # see if the start and end projects have mesh terms against them
        # return bool
        return start, end

    def prepare(self):
        # mysql setup
        # db = 'production' if not self.test else 'dev'
        db = 'dev'  # *****just in case - remove after testing*******

        # elasticsearch setup
        # es_mode = 'rwjf_prod' if not self.test else 'rwjf_dev'
        es_mode = 'rwjf_dev'  # *****just in case - remove after testing*******
        es_config = misctools.get_config('elasticsearch.config', es_mode)
        es = Elasticsearch(es_config['external_host'], port=es_config['port'])

        # s3 setup and file key collection
        bucket = 'innovation-mapping-general'
        key_prefix = 'nih_abstracts_processed/mti'
        keys = self.get_abstract_file_keys(bucket, key_prefix)
        for k in keys:
            print(self.done_check(es, index=es_config['index'],
                  doc_type=es_config['type'], key=k))
            break  # just testing

        # TODO: replace all this
        batches = self.batch_limits(project_query, BATCH_SIZE)
        job_params = []
        for start, end in batches:
            params = {'start_index': start,
                      'end_index': end,
                      'config': "mysqldb.config",
                      'db': db,
                      'outinfo': es_config['internal_host'],
                      'out_port': es_config['port'],
                      'out_index': es_config['index'],
                      'out_type': es_config['type'],
                      'done': es.exists(index=es_config['index'],
                                        doc_type=es_config['type'],
                                        id=end)
                      }
            print(params)
            job_params.append(params)
        return job_params

    def combine(self, job_params):
        self.output().touch()


if __name__ == '__main__':
    date = datetime.date(year=2018, month=12, day=25)
    process = AbstractsMeshTask(test=True, batchable='', job_def='', job_name='',
            job_queue='', region_name='', db_config_path='MYSQLDB', date=date,
            _routine_id='')
    process.prepare()
