from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import logging
from numpy.random import randint
import os

from nesta.packages.nlp_utils import (clean_and_tokenize,
        build_ngrams, filter_by_idf)


def dummy_predict(text):
    ''' dummy_predict
    Creates a random set of numbers representing between 1 and 3
    SDGs

    Args:
        text (str)

    Returns:
        predictions (list)
    '''
    goals = list(range(1, 18))
    n_goals = randint(1, 4)

    predictions =[]
    for _ in n_goals:
        predictions.append(goals.pop(randint(len(goals))))

    return predictions

def preprocess(text):
    return text

def retrieve_unsdg_ids(bucket, id_file):
    ''' retrieve_unsdg_ids
    Retrieves ids of documents to be labelled from an S3 bucket.

    Args:
        bucket (str): s3 bucket
        key (str): s3 key

    Returns:
        (list): list of ids
    '''
    target = f's3://{bucket}/{id_file}'
    logging.debug(f'Retrieving ids from S3: {id_file}')

    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, id_file)
    return obj.get()['Body'].read().splitlines()

def get_text_with_id(doc_id):
    return 'dummy text'

def run():
    logging.getLogger().setLevel(logging.WARNING)

    id_bucket = os.environ["BATCHPAR_id_bucket"]
    id_key = os.environ["BATCHPAR_id_key"]
    model_bucket = os.environ["BATCHPAR_model_bucket"]
    model_key_prefix = os.environ["BATCHPAR_model_key_prefix"]
    model_date = os.environ["BATCHPAR_model_date"]
    es_config = os.environ["BATCHPAR_outinfo"]

    doc_ids = retrieve_unsdg_ids(id_bucket, id_file)
    
    unsdg_docs = []
    for doc_id in doc_ids:
        text = get_text_with_id(doc_id)
        text = preprocess(text)

        predictions = dummy_predict(text)

        unsdg_docs.append(
                {
                    'doc_id': doc_id,
                    'date_unsdg_model': model_date,
                    'terms_unsdg_abstract': predictions
                    }
                )

        es = Elasticsearch(
                es_config['internal_host'],
                port=es_config['port'],
                sniff_on_start=True
                )
        logging.warning(f'writing {len(unsdg_docs)} documents to elasticsearch')
        for doc in unsdg_docs:
            uid = doc.pop("doc_id")
            existing = es.get(es_config['index'], doc_type=es_config['type'], id=uid)['_source']
            doc = {**existing, **doc}
            es.index(es_config['index'], doc_type=es_config['type'], id=uid, body=doc)


if __name__ == "__main__":
    run()
