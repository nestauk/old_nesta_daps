'''
Clio Task
==========

Process/enrich data to be searchable with topics.
'''

import luigi
from nesta.production.luigihacks import s3
from nesta.production.luigihacks import luigi_logging
from nesta.production.luigihacks.automl import AutoMLTask
from nesta.production.luigihacks.misctools import get_config
import os
import json
from nesta.production.luigihacks.elasticsearchplus import ElasticsearchPlus
from nesta.production.luigihacks.s3task import S3Task
from datetime import datetime

S3INTER = "s3://clio-data/{dataset}/{date}/"
S3PREFIX = ("s3://clio-data/{dataset}/"
            "{phase}/{dataset}_{phase}.json")

THIS_PATH = os.path.dirname(os.path.realpath(__file__))
CHAIN_PARAMETER_PATH = os.path.join(THIS_PATH,
                                    "clio_process_task_chain.json")

def clean(dirty_json, dataset, text_threshold=50):
    """Standardise a json data's schema. This function will identify
    four types of entity: the title (must be called 'title' in the input
    json), the id (must be called 'id' in the input json), text bodies
    (will be searchable in Clio, required to have at least 'text_threshold'
    characters), and 'other' fields which is everything else.

    Args:
        dirty_json (json): Input raw data json.
        dataset (str): Name of the dataset.
        text_threshold (int): Minimum number of characters required
                              to define a body of text as a textBody field.
    Returns:
        uid, cleaned_json, fields: list of ids, cleaned json, set of fields
    """
    uid = []
    cleaned_json = []
    fields = set()
    for row in dirty_json:
        new_row = dict()
        for field, value in row.items():
            # Ignore nulls
            if value is None:
                continue
            if field == "title":
                field = f"title_of_{dataset}"
            elif field == "id":
                field = f"id_of_{dataset}"
                uid.append(value)
            elif type(value) is str and len(value) > text_threshold:
                field = f"textBody_{field}_{dataset}"
            else:
                field = f"other_{field}_{dataset}"
            new_row[field] = value
            fields.add(field)
        cleaned_json.append(new_row)
    return uid, cleaned_json, fields


class ClioTask(luigi.Task):
    """Clean input data, launch AutoML, and use AutoML outputs to enrich ES injection.

    Args:
         dataset (str): The dataset's name, for book-keeping.
         production (bool): Whether or not to run in non-test mode.
         verbose (bool): Whether or not to print lots.
         write_es (bool): Whether or not to write data to ES (AutoML will still be run.)
    """
    dataset = luigi.Parameter()
    date = luigi.DateParameter(default=datetime.today())
    production = luigi.BoolParameter(default=False)
    verbose = luigi.BoolParameter(default=False)
    write_es = luigi.BoolParameter(default=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_path_in = S3PREFIX.format(dataset=self.dataset,
                                          phase="raw_data")

    def requires(self):
        """Yield AutoML"""
        test = not self.production
        luigi_logging.set_log_level(test, self.verbose)
        return AutoMLTask(s3_path_prefix=S3INTER.format(dataset=self.dataset, date=self.date),
                          task_chain_filepath=CHAIN_PARAMETER_PATH,
                          input_task=S3Task,
                          input_task_kwargs={'s3_path':self.s3_path_in},
                          final_task='corex_topic_model',
                          test=test)

    def run(self):
        """Write data to ElasticSearch if required"""
        if not self.write_es:
            return

        # Read the topics data
        file_ptr = self.input().open("rb")
        path = file_ptr.read()
        file_ptr.close()

        file_io_topics = s3.S3Target(f's3://clio-data/{path.decode("utf-8")}').open("rb")
        topic_json = json.load(file_io_topics)
        file_io_topics.close()
        
        # Read the raw data
        file_io_input = s3.S3Target(self.s3_path_in).open("rb")
        dirty_json = json.load(file_io_input)
        file_io_input.close()

        uid, cleaned_json, fields = clean(dirty_json, self.dataset)

        # Assign topics
        assert len(cleaned_json) == len(topic_json)
        for row, topic_data in zip(cleaned_json, 
                                   topic_json['data']['rows']):
            topics = [k for k, v in topic_data.items()
                      if k != 'id' and v >= 0.2]
            row[f"terms_of_{self.dataset}"] = topics
        fields.add(f"terms_of_{self.dataset}")

        # Prepare connection to ES
        prod_label = '' if self.production else '_dev'
        es_config = get_config('elasticsearch.config', 'clio')
        es_config['index'] = f"clio_{self.dataset}{prod_label}"
        es = ElasticsearchPlus(entity_type=self.dataset,
                               aws_auth_region=es_config.pop('region'),
                               country_detection=True,
                               caps_to_camel_case=True,
                               **es_config)

        # Dynamically generate the mapping based on a template
        with open("clio_mapping.json") as f:
            mapping = json.load(f)
        for f in fields:
            kwargs = {}
            _type = "text"
            if f.startswith("terms"):
                kwargs = {"fields": {"keyword":
                                     {"type": "keyword"}},
                          "analyzer": "terms_analyzer"}
            elif not f.startswith("textBody"):
                _type = "keyword"
            mapping["mappings"]["_doc"]["properties"][f] = dict(type=_type,
                                                                **kwargs)

        # Drop, create and send data
        es.indices.delete(index=es_config['index'])
        es.indices.create(index=es_config['index'], body=mapping)
        for id_, row in zip(uid, cleaned_json):
            es.index(index=es_config['index'], doc_type=es_config['type'],
                     id=id_, body=row)


        # Drop, create and send data
        topic_idx = f"{es_config['index']}_topics"
        es.indices.delete(index=topic_idx)
        es.indices.create(index=topic_idx)
        # for id_, row in zip(uid, cleaned_json):
        #     es.index(index=es_config['index'], doc_type=es_config['type'],
        #              id=id_, body=row)
