from gensim.corpora import Dictionary
from collections import Counter
import itertools  
import numpy as np

import os
import boto3
import pandas as pd
import json
from nesta.production.luigihacks.s3 import parse_s3_path
from ast import literal_eval

def term_counts(dct, row, binary=False):
    """Convert a single single document to term counts via
    a gensim dictionary.
    
    Args:
        dct (Dictionary): Gensim dictionary.
        row (str): A document.
        binary (bool): Binary rather than total count?
    Returns:
        dict of term id (from the Dictionary) to term count.
    """
    return {dct[idx]: (count if not binary else 1)
            for idx, count in Counter(dct.doc2idx(row)).items()
            if idx != -1}        

def optional(name, default):
    """Defines optional env fields with default values"""
    var = f'BATCHPAR_{name}'
    try:
        return (default if var not in os.environ 
                else literal_eval(os.environ[var]))
    except ValueError:
        return os.environ[var]

def run():
    s3_path_in = os.environ['BATCHPAR_s3_path_in']
    text_field = optional('text_field', 'body')
    id_field = optional('id_field', 'id')
    binary = optional('binary', False)
    min_df = optional('min_df', 1)
    max_df = optional('max_df', 1.0)

    # Load the chunk                                      
    s3 = boto3.resource('s3')
    s3_obj_in = s3.Object(*parse_s3_path(s3_path_in))    
    data = json.load(s3_obj_in.get()['Body'])

    # Extract what you need from the data
    _data = [list(itertools.chain.from_iterable(row[text_field]))
             for row in data if type(row[text_field]) is list]
    index = [row[id_field] for row in data]
    del data

    # Build the corpus
    dct = Dictionary(_data)
    dct.filter_extremes(no_below=np.ceil(min_df*len(_data)), 
                        no_above=max_df)

    # Write the data as JSON
    body = json.dumps([dict(id=idx, **term_counts(dct, row, binary))
                       for idx, row in zip(index, _data)])
    del _data
    del index
    del dct
    
    # Mark the task as done and save the data             
    if "BATCHPAR_outinfo" in os.environ:
        s3_path_out = os.environ["BATCHPAR_outinfo"]
        s3 = boto3.resource('s3')
        s3_obj = s3.Object(*parse_s3_path(s3_path_out))
        s3_obj.put(Body=body)

if __name__ == "__main__":
    if "BATCHPAR_outinfo" not in os.environ:
        os.environ["BATCHPAR_text_field"] = 'abstractText'
        os.environ["BATCHPAR_binary"] = 'True'
        os.environ["BATCHPAR_min_df"] = '0.001'
        os.environ["BATCHPAR_s3_path_in"] = ('s3://clio-data/gtr/'
                                             'NGRAM.TEST_True.json')
    run()
