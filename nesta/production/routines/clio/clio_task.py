'''                                                            
S3 Example                                                     
==========                                                     
                                                               
An example of building a pipeline with S3 Targets              
'''

import luigi
from nesta.production.luigihacks import s3
from nesta.production.routines.automl.automl import AutoMLTask
from nesta.production.luigihacks.misctools import get_config
import os
import logging
import json
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection
import boto3


S3INTER = ("s3://clio-data/{dataset}/")
S3PREFIX = ("s3://clio-data/{dataset}/{phase}/"
            "{dataset}_{phase}.json")

THIS_PATH = os.path.dirname(os.path.realpath(__file__))
CHAIN_PARAMETER_PATH = os.path.join(THIS_PATH,
                                    "clio_process_task_chain.json")

def clean(dirty_json, dataset, text_threshold=50):

    uid = []
    cleaned_json = []
    fields = set()
    for row in dirty_json:        
        new_row = dict()
        for field, value in row.items():
            if value is None:
                continue
            if field == "title":
                field = f"title_of_{dataset}"
            elif field == "id":
                field = f"id_of_{dataset}"
                uid.append(value)
            elif type(value) is str and len(value) > 50:
                field = f"textBody_{field}_{dataset}"
            else:
                field = f"other_{field}_{dataset}"
            new_row[field] = value
            fields.add(field)
        cleaned_json.append(new_row)
    return uid, cleaned_json, fields


class ClioTask(luigi.Task):
    dataset = luigi.Parameter()    
    production = luigi.BoolParameter(default=False)
    verbose = luigi.BoolParameter(default=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_path_in = S3PREFIX.format(dataset=self.dataset,
                                          phase="raw_data")
    

    def requires(self):
        test = not self.production
                
        if test or self.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
            logging.getLogger("luigi-interface").setLevel(logging.DEBUG)

        if not self.verbose:
            logging.getLogger("urllib3").setLevel(logging.WARNING)
            logging.getLogger("botocore").setLevel(logging.WARNING)
            logging.getLogger("boto3").setLevel(logging.WARNING)
            logging.getLogger("luigi-interface").setLevel(logging.WARNING)

        yield AutoMLTask(s3_path_in=self.s3_path_in,
                         s3_path_prefix=S3INTER.format(dataset=self.dataset),
                         task_chain_filepath=CHAIN_PARAMETER_PATH,
                         test=test)

    def run(self):  
        # Unused for the moment
        file_ios = {child: s3.S3Target(params["s3_path_out"])
                    for child, params in AutoMLTask.task_parameters.items()}
        
        # Read the raw data
        file_io_input = s3.S3Target(self.s3_path_in).open("rb")
        dirty_json = json.load(file_io_input)
        file_io_input.close()
        
        # Read the topics data
        file_io_topics = file_ios["topic_model"].open("rb")
        topic_json = json.load(file_io_topics)
        file_io_topics.close()
        
        # Clean the field names
        uid, cleaned_json, fields = clean(dirty_json, self.dataset)
        
        # Assign topics
        assert len(cleaned_json) == len(topic_json)
        for row, topics in zip(cleaned_json, topic_json):
            row[f"terms_of_{self.dataset}"] = topics
        fields.add(f"terms_of_{self.dataset}")

        # Prepare connection to ES
        prod_label = '' if self.production else '_dev'
        es_config = get_config('elasticsearch.config', 'clio')
        es_config['index'] = f"clio_{self.dataset}{prod_label}"
        credentials = boto3.Session().get_credentials()
        awsauth = AWS4Auth(credentials.access_key, credentials.secret_key,
                           es_config['region'], 'es')
        
        es = Elasticsearch(es_config['host'],
                           port=int(es_config['port']),
                           http_auth=awsauth,
                           use_ssl=True,
                           verify_certs=True,
                           connection_class=RequestsHttpConnection)


        # Dynamically generate the mapping based on a template
        with open("clio_mapping.json") as f:
            mapping = json.load(f)

        for f in fields:
            _type = "keyword"
            kwargs = {}
            if f.startswith("textBody"):
                _type = "text"
            elif f.startswith("terms"):
                _type = "text"
                kwargs = {"fields": {"keyword": {"type": "keyword"}},
                          "analyzer": "terms_analyzer"}
            mapping["mappings"]["_doc"]["properties"][f] = dict(type=_type, **kwargs)
        print(mapping)

        # Drop, create and send data
        es.indices.delete(index=es_config['index'])
        es.indices.create(index=es_config['index'], body=mapping)
        for id_, row in zip(uid, cleaned_json):
            es.index(es_config['index'], doc_type=es_config['type'],
                     id=id_, body=row)
            
