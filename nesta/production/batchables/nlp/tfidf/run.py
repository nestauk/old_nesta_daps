from sklearn.feature_extraction.text import TfidfVectorizer
from nesta.production.luigihacks.s3 import parse_s3_path

import os
import boto3
import json
import numpy as np
import json


def run():
    # Get variables out
    s3_path_in = os.environ['BATCHPAR_s3_path_in']
    s3_path_out = os.environ["BATCHPAR_outinfo"]
    first_index = int(os.environ['BATCHPAR_first_index'])
    last_index = int(os.environ['BATCHPAR_last_index'])
    lower_tfidf_percentile = int(os.environ['BATCHPAR_lower_tfidf_percentile'])
    upper_tfidf_percentile = int(os.environ['BATCHPAR_upper_tfidf_percentile'])

    # Load the data
    s3 = boto3.resource('s3')
    s3_obj_in = s3.Object(*parse_s3_path(s3_path_in))
    data = json.load(s3_obj_in.get()['Body'])

    # Create a "corpus" by joining together text fields
    # which have been analysed by the ngrammer already
    corpus = []
    for row in data[first_index, last_index]
        doc = []
        for k, v in row.items():
            if not (type(v) is list):
                continue
            doc += [" ".join(item) for item in v]
        corpus.append(" ".join(doc))

    # Calculate tfidf values for all terms
    tvec = TfidfVectorizer()
    _transformed = tvec.fit_transform(corpus)

    # Extract a reverse lookup for indexes to terms
    lookup = {idx: term for term, idx in tvec.vocabulary_.items()}
    
    # Calculate the lower and upper bounds from the percentiles
    tfidf_values = _transformed[_transformed > 0]
    lower_cut = np.percentile(tfidf_values, lower_tfidf_percentile)
    upper_cut = np.percentile(tfidf_values, upper_tfidf_percentile)

    # Generate the list of allowed terms for each document
    good_words_corpus = []
    for row in _transformed.toarray():
        good_words_doc = set(lookup[idx] for idx, value in enumerate(row)
                             if (value > lower_cut) and (value < upper_cut))
        good_words_corpus.append(good_words_doc)

    # Finally, filter the input data
    outdata = []
    for row, good_words in zip(data[first_index, last_index], good_words_corpus):
    new_row = dict(**row)
    for k, v in row.items():
        if not (type(v) is list):
            continue
        new_row[k] = [" ".join(term for term in sentence 
                               if term in good_words)
                      for sentence in v]
        outdata.append(new_row)

    # Mark the task as done
    if s3_path_out != "":
        s3 = boto3.resource('s3')
        s3_obj = s3.Object(*parse_s3_path(s3_path_out))
        s3_obj.put(Body=json.dumps(processed))


if __name__ == "__main__":
    # Local testing
    if "BATCHPAR_outinfo" not in os.environ:
        os.environ['BATCHPAR_s3_path_in'] = "s3://clio-data/gtr/raw_data/gtr_raw_data.json"
        os.environ['BATCHPAR_outinfo'] = "s3://clio-data/gtr/intermediate/gtr/raw_data_to_gtr/processed_data_ngram.0-10.json"
        os.environ['BATCHPAR_first_index'] = '0'
        os.environ["BATCHPAR_last_index"] = '10'
    run()
