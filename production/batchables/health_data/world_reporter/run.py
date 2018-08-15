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


# Path to the abstract element                                                                          
ELEMENT_EXISTS = EC.visibility_of_element_located((By.CSS_SELECTOR,
                                                   ("#dataViewTable > tbody > "
                                                    "tr.expanded-view.ng-scope > "
                                                    "td > div > div > p")))

def run():

    #
    obj = s3.Object("nesta-inputs", os.environ["BATCHPAR_s3inpath"])    
    df = pd.read_csv(obj.get()['Body'])
    obj.delete()

    df.columns = [col.replace(" ","_").lower() for col in df.columns]
    # Create a field ready for the abstract text                                                        
    df["abstract_text"] = None

    # Start the display
    display = Display(visible=0, size=(1366, 768))
    display.start()

    # Get the data for each abstract link                                                               
    for idx, row in df.iterrows():
        abstract = get_abstract(url=row["abstract_link"])
        _abstract = [tokenize_document(a) for a in abstract]
        processed_abstract = build_ngrams(_abstract)
        df.at[idx, "abstract"] = abstract
        df.at[idx, "processed_abstract"] = processed_abstract
    print(df)

    #es = Elasticsearch([os.environ["BATCHPAR_host"]], port=443, scheme="https")
    

    # Tidy up
    display.stop()


if __name__ == "__main__":
    run()
