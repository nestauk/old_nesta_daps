import ast
import boto3
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import logging
from numpy.random import randint
import os
import pickle
from gensim.models.ldamodel import LdaModel
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from nesta.packages.nlp_utils.ngrammer import Ngrammer
from nesta.packages.nlp_utils.preprocess import clean_and_tokenize
from nesta.packages.s3_utils.s3_transfer import get_pkl_object, get_presigned_url

from nesta.packages.unsdg_nlp_utils.bigram_gensim_creation import generate_bigrams, clean_bigrams
from nesta.packages.unsdg_nlp_utils.preprocess_spacy import *
from nesta.packages.nlp_utils.topic_distribution_lda import get_distribution_probabilities

def dummy_model(text):
    ''' dummy_model
    Emulates the response from a UNSDG classifier.
    '''
    goals = list(range(1, 18))
    n_goals = randint(1, 3)

    predictions = []
    for _ in range(n_goals):
        predictions.append(goals.pop(randint(len(goals))))

    return predictions

def predict_sdg_labels(topic_dist, model_dictionary):

    sd_labels_predictions = []
    for sdg_label, model in model_dictionary.items():
        prediction = model.predict(topic_dist)
        sd_labels_predictions.extend(list(prediction))

    sdg_list = [i+1 for i, x in enumerate(sdg_labels_predict) if x == 1]
    sdg_list = [i+1 for i in indices]
    return sdg_list

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

def get_abstract_by_id(es_client, index, doc_type, doc_id):
    ''' get_abstract_by_id

    Args:
        es_client (object): instantiated elasticsearch client
	index (str): name of the index
	doc_type (str): name of the document type
	idx (int): id of the document

    Returns:
        abstract (str): abstract of the document
    '''

    res = es_client.get(index=index, doc_type=doc_type, id=doc_id)
    abstract = res['_source'].get('textBody_abstract_project')
    return abstract

def run():
    logging.getLogger().setLevel(logging.WARNING)

    ids_bucket = os.environ["BATCHPAR_id_bucket"]
    ids_key = os.environ["BATCHPAR_id_key"]
    model_bucket = os.environ["BATCHPAR_model_bucket"]
    model_key = os.environ["BATCHPAR_model_key"]
    model_date = os.environ["BATCHPAR_model_date"]
    dictionary_key = os.environ["BATCHPAR_dictionary_key"]
    phraser_key = os.environ["BATCHPAR_phraser_key"]
    topic_model_key = os.environ["BATCHPAR_topic_model_key"]
    stop_words_key = os.environ["BATCHPAR_stop_words_key"]

    es_config = ast.literal_eval(os.environ["BATCHPAR_outinfo"])

    ids = retrieve_id_file(ids_bucket, ids_key)
    model = get_pkl_object(model_bucket, model_key)
    dictionary = get_pkl_object(model_bucket, dictionary_key)
    bigram_phraser = get_pkl_object(model_bucket, phraser_key)
    sdg_stop_words = get_pkl_object(model_bucket, stop_words_key)
    lda_model = LdaModel.load(get_presigned_url(model_bucket, topic_model_key))


    es = Elasticsearch(es_config['internal_host'], port=es_config['port'], sniff_on_start=True)

    #load stop words and update spacy.nlp vocab
    #load stopwords
    spacy_nlp_vocab_update(sdg_stop_words)


    doc_predictions = []
    for doc_id in ids:
        # query ES for text of record with id
        abstract = get_abstract_by_id(
                es,
                index=es_config['index'],
                doc_type=es_config['type'],
                doc_id=doc_id
                )
        if abstract is not None:
            # preprocess text
            abstract_tokens = word_tokenise(abstract)

            #load bigram model

            tokens_unigrams_bigrams = generate_bigrams(abstract_tokens, bigram_phraser)
            clean_tokens = clean_bigrams(tokens_unigrams_bigrams, sdg_stop_words)

            #load lda model and dictionary
            doc_term_vec = [dictionary.doc2bow(doc) for doc in [clean_tokens]]
            ct = lda_model[doc_term_vec]
            topic_distrib = get_distribution_probabilities(ct)

            #load models and predict SDGs
            predictions = predict_sdg_labels(topic_distrib, model)

            # insert into ES
            doc_predictions.append(
                    {'doc_id': doc_id,
                     'date_unsdg_model': model_date,
                     'terms_unsdg_abstract': predictions,
                     }
                )

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
