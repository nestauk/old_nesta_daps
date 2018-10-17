import pandas as pd
import os
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import boto3

from nesta.packages.health_data.process_nih import _extract_date
from nesta.packages.health_data.process_nih import geocode_dataframe
from nesta.packages.health_data.process_nih import country_iso_code_dataframe
from nesta.packages.decorators.schema_transform import schema_transform
from nesta.production.orms.orm_utils import get_mysql_engine


# @schema_transform(mapping, from_key, to_key)
def prepare(self):
    # collect data from database
    engine = get_mysql_engine("MYSQLDB", "mysqldb", "dev")
    # TODO: are these all the columns we want in ES?
    cols = ['application_id', 'org_city', 'org_country',
            'project_start', 'project_end']
    df = pd.read_sql('nih_projects', engine, columns=cols).head(30)
    df.columns = [c.lstrip('org_') for c in df.columns]

    # For sense-checking later
    n = len(df)
    n_ids = len(set(df.application_id))

    # Geocode the dataframe
    df = geocode_dataframe(df)
    # clean start and end dates
    for col in ["project_start", "project_end"]:
        df[col] = df[col].apply(_extract_date)
    # append iso codes for country
    df = country_iso_code_dataframe(df)
    assert len(set(df.application_id)) == n_ids
    assert len(df) == n

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
        #_abstract = tokenize_document(abstract.decode("utf-8")) 
        #processed_abstract = build_ngrams([_abstract])
        df.at[idx, "abstract_text"] = abstract.decode("utf-8")
        #        df.at[idx, "processed_abstract_text"] = processed_abstract

    es = Elasticsearch([os.environ["BATCHPAR_outinfo"]], 
                       port=443, scheme="https")
    for _, row in df.iterrows():
        doc = dict(row.loc[~pd.isnull(row)])
        doc.pop("unnamed:_0")
        uid = doc.pop("unique_id")
        res = es.index(index='rwjf_uid', doc_type='world_reporter', 
                       id=uid, body=doc)

    # Tidy up
    display.stop()


if __name__ == "__main__":
    run()
