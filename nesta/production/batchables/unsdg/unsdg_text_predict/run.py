import boto3
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import logging
from numpy.random import randint
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from nesta.packages.nlp_utils.ngrammer import Ngrammer
from nesta.packages.nlp_utils.preprocess import (clean_and_tokenize,
        build_ngrams, filter_by_idf)


def dummy_model(text):
    ''' dummy_model
    Emulates the response from a UNSDG classifier.
    '''
    goals = list(range(1, 18))
    n_goals = randint(1, 3)

    predictions = []
    for _ in n_goals:
        predictions.append(goals.pop(randint(len(goals))))

    return predictions

def retrieve_id_file(bucket, key):
    ''' retrieve_id_file
    Returns list of doc ids from a text file.
    
    Args:
        bucket (str): s3 bucket
        key (str): key for an s3 object

    Returns:
        ids (:obj:`list` of :obj:`int`): list of ids
    '''
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    filestream = obj.get()['Body']
    ids = [int(i) for i in filestream.read().splitlines()]
    return ids

def get_abstract_by_id():
    pass

def preprocess(text):
    return text

def put_unsdg_terms(labels):
    pass

def run():
    logging.getLogger().setLevel(logging.WARNING)

    ids_bucket = os.environ["BATCHPAR_id_bucket"]
    ids_key = os.environ["BATCHPAR_id_key"]
    model_bucket = os.environ["BATCHPAR_model_bucket"]
    model_key_prefix = os.environ["BATCHPAR_model_key_prefix"]
    model_date = os.environ["BATCHPAR_model_date"]
    es_config = os.environ["BATCHPAR_outinfo"]
    
    ids = retrieve_id_file(ids_bucket, ids_key)
    
    doc_predictions = []
    for doc_id in ids:
        # query ES for text of record with id
        # preprocess text
        text = preprocess('dummy text')
        # predict SDGs
        predictions = dummy_model(text)
        # insert into ES
        put_unsdg_terms(predictions)
        doc_predictions.append(
                {'doc_id': doc_id,
                 'date_unsdg_model': model_date,
                 'terms_unsdg_': predictions,
                 }
            )

    es = Elasticsearch(es_config['internal_host'], port=es_config['port'], sniff_on_start=True)
    logging.warning(f'writing {len(doc_predictions)} documents to elasticsearch')
    for doc in doc_predictions:
        uid = doc.pop("doc_id")
        try:
            existing = es.get(es_config['index'], doc_type=es_config['type'], id=uid)['_source']
        except NotFoundError:
            logging.warning(f"Missing project for abstract: {uid}")
        else:
            doc = {**existing, **doc}
            es.index(es_config['index'], doc_type=es_config['type'], id=uid, body=doc)


    logging.info('finished')

if __name__ == '__main__':
    run()
