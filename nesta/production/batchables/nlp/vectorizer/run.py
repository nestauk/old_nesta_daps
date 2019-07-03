import os
import boto3
from sklearn.feature_extraction.text import CountVectorizer
import pandas as pd
import json
from nesta.production.luigihacks.s3 import parse_s3_path
from ast import literal_eval

def optional(name, default):
    var = f'BATCHPAR_{name}'
    return (default if var not in os.environ 
            else literal_eval(os.environ[var]))

def run():
    s3_path_in = os.environ['BATCHPAR_s3_path_in']
    binary = optional('binary', False)
    min_df = optional('min_df', 1)
    max_df = optional('max_df', 1.0)

    # Load the chunk                                      
    s3 = boto3.resource('s3')
    s3_obj_in = s3.Object(*parse_s3_path(s3_path_in))
    data = json.load(s3_obj_in.get()['Body'])

    # Prepare data and vectorizer
    _data = [' '.join(' '.join(para) for para in row['body'])
             for row in data]
    vtzr = CountVectorizer(binary=binary, 
                           min_df=min_df, 
                           max_df=max_df)                 

    # Generate inputs for pandas
    index = [row['id'] for row in data]
    data = [list(row.toarray()[0]) 
            for row in vtzr.fit_transform(_data)]
    columns = vtzr.get_feature_names()
    
    # Generate dataframe
    df = pd.DataFrame(data=data, columns=columns, 
                      index=index)
    
    # Write the dataframe as JSON
    body = json.dumps([{'id':index, **row.dropna().to_dict()} 
                       for index,row in df.iterrows()])


    # Mark the task as done and save the data             
    if "BATCHPAR_outinfo" in os.environ:
        s3_path_out = os.environ["BATCHPAR_outinfo"]
        s3 = boto3.resource('s3')
        s3_obj = s3.Object(*parse_s3_path(s3_path_out))
        s3_obj.put(Body=body)

if __name__ == "__main__":
    if "BATCHPAR_outinfo" not in os.environ:
        os.environ["BATCHPAR_s3_path_in"] = 's3://nesta-arxlive/automl/2019-07-03/NGRAM.TEST_True-0.json'
        os.environ["BATCHPAR_binary"] = 'True'
        os.environ["BATCHPAR_min_df"] = '0.001'
    run()
