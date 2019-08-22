import pandas as pd
import json
from itertools import chain
from scipy.sparse import csr_matrix
from corextopic import corextopic as ct
from nesta.core.luigihacks.s3 import parse_s3_path
import os
import boto3
from ast import literal_eval

def run():
    s3_path_in = os.environ['BATCHPAR_s3_path_in']
    n_hidden = int(literal_eval(os.environ['BATCHPAR_n_hidden']))

    # Load and shape the data
    s3 = boto3.resource('s3')
    s3_obj_in = s3.Object(*parse_s3_path(s3_path_in))
    data = json.load(s3_obj_in.get()['Body'])    
    # df = pd.DataFrame(data).fillna(0)
    # del data
    # X = csr_matrix(df.drop('id', axis=1).values)

    ids = []
    indptr = [0]
    indices = []
    counts = []
    vocab = {}
    for row in data:
        ids.append(row.pop('id'))
        for term, count in row.items():
            idx = vocab.setdefault(term, len(vocab))
            indices.append(idx)
            counts.append(count)
        indptr.append(len(indices))
    X = csr_matrix((counts, indices, indptr), dtype=int)
    _vocab = {v:k for k, v in vocab.items()}

    # Fit the model
    topic_model = ct.Corex(n_hidden=n_hidden)
    topic_model.fit(X)
    topics = topic_model.get_topics()

    # Generate topic names
    #topic_names = {f'topic_{itop}': [df.columns[idx]
    topic_names = {f'topic_{itop}': [_vocab[idx]
                                     for idx, weight in topic]
                   for itop, topic in enumerate(topics)}

    # Generate output data rows
    #rows = [{f'topic_{itop}': sum(row[idx]*weight
    rows = [{f'topic_{itop}': sum(row.getcol(idx).toarray()[0][0]*weight
                                  for idx, weight in topic)
             for itop, topic in enumerate(topics)}
            for row in X]
            #for _, row in df.iterrows()]
    rows = [dict(id=id, **{k: v for k, v in row.items()
                           if v > 0})
            for id, row in zip(ids, rows)]
            #for id, row in zip(df['id'], rows)]

    # Curate the output
    output = {'loss': topic_model.tc,
              'data': {'topic_names': topic_names,
                       'rows': rows}}

    # Mark the task as done and save the data
    if "BATCHPAR_outinfo" in os.environ:
        s3_path_out = os.environ["BATCHPAR_outinfo"]
        s3 = boto3.resource('s3')
        s3_obj = s3.Object(*parse_s3_path(s3_path_out))
        s3_obj.put(Body=json.dumps(output))

if __name__ == "__main__":
    if "BATCHPAR_outinfo" not in os.environ:
        os.environ["BATCHPAR_s3_path_in"] = 's3://nesta-arxlive/automl/2019-07-04/VECTORIZER.binary_True.min_df_0.001.NGRAM.TEST_True-0_5164.json'
        #os.environ["BATCHPAR_s3_path_in"] = 's3://nesta-arxlive/automl/2019-07-07/COREX_TOPIC_MODEL.n_hidden_34.0.VECTORIZER.binary_True.min_df_0.001.NGRAM.TEST_False-0_307504.json'
        os.environ["BATCHPAR_n_hidden"] = '39'
    run()
