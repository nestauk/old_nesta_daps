#TODO: submit this and check a) it works and b) the data structure
# Then index data according to https://stackoverflow.com/questions/20288770/how-to-use-bulk-api-to-store-the-keywords-in-es-by-using-python

from pyvirtualdisplay import Display
from health_data.world_reporter import get_abstract
from nlp_utils.preprocess import tokenize_document
from nlp_utils.preprocess import build_ngrams
import pandas as pd
import os
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import boto3

s3 = boto3.resource('s3')

def run():

    obj = s3.Object("nesta-inputs", os.environ["BATCHPAR_in_path"])
    df = pd.read_csv(obj.get()['Body'], encoding="utf-8")
    obj.delete()

    df.columns = [col.replace(" ","_").lower() for col in df.columns]

    # Create a field ready for the abstract text 
    df["abstract_text"] = None
    #df["processed_abstract_text"] = None

    # Start the display
    display = Display(visible=0, size=(1366, 768))
    display.start()

    # Get the data for each abstract link                                      
    for idx, row in df.iterrows():
        abstract = get_abstract(url=row["abstract_link"])
        _abstract = tokenize_document(abstract.decode("utf-8")) 
        processed_abstract = build_ngrams([_abstract])
        df.at[idx, "abstract_text"] = abstract.decode("utf-8")
        #        df.at[idx, "processed_abstract_text"] = processed_abstract

    es = Elasticsearch([os.environ["BATCHPAR_outinfo"]], 
                       port=443, scheme="https")
    for _, row in df.iterrows():
        doc = dict(row.loc[~pd.isnull(row)])
        doc.pop("unnamed:_0")
        res = es.index(index='rwjf', doc_type='world_reporter', 
                       id=row["program_number"], body=doc)

    # Tidy up
    display.stop()


if __name__ == "__main__":
    run()
