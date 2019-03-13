from sklearn.feature_extraction.text import TfidfVectorizer
from nesta.production.luigihacks.s3 import parse_s3_path

import os
import boto3
import json
import numpy as np
import json


def chunker(_transformed, n_chunks):
    n_rows, _ = _transformed.shape
    chunk_size = round(n_rows / n_chunks)
    remaining = n_rows % chunk_size

    for i in range(0, n_chunks):
        chunk = _transformed[i*chunk_size: (i+1)*chunk_size].toarray()
        for row in chunk:
            yield row
    for row in _transformed[n_rows-remaining:].toarray():
        yield row


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
    for row in data[first_index: last_index]:
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
    print((_transformed > 0).sum(), lower_cut, upper_cut, tfidf_values.min(), tfidf_values.max())
    del tfidf_values

    # Generate the list of allowed terms for each document
    good_words_corpus = []
    for row in chunker(_transformed, 100):
        good_words_doc = set(lookup[idx] for idx, value in enumerate(row)
                             if (value > lower_cut) and (value < upper_cut))
        good_words_corpus.append(good_words_doc)

    # Finally, filter the input data
    outdata = []
    for row, good_words in zip(data[first_index: last_index], good_words_corpus):
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
        s3_obj.put(Body=json.dumps(outdata))
    else:
        return outdata


if __name__ == "__main__":
    # Local testing
    if "BATCHPAR_outinfo" not in os.environ:
        os.environ['BATCHPAR_s3_path_in'] = "s3://clio-data/gtr/ngram/NGRAM.test_False.json"
        os.environ['BATCHPAR_outinfo'] = ""
        os.environ['BATCHPAR_first_index'] = '0'
        os.environ["BATCHPAR_last_index"] = '20000'
        os.environ["BATCHPAR_upper_tfidf_percentile"] = '90'
        os.environ["BATCHPAR_lower_tfidf_percentile"] = '5'
        
        
    run()
