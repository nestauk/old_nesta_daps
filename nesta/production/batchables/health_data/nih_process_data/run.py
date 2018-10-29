from elasticsearch import Elasticsearch
import os
import pandas as pd
from sqlalchemy.orm import sessionmaker

from nesta.packages.decorators.schema_transform import schema_transformer
from nesta.packages.health_data.process_nih import _extract_date
from nesta.packages.health_data.process_nih import country_iso_code_dataframe
from nesta.packages.health_data.process_nih import geocode_dataframe
from nesta.production.orms.orm_utils import get_mysql_engine
from nesta.production.orms.nih_orm import Projects


def run():
    start_index = os.environ["BATCHPAR_start_index"]
    end_index = os.environ["BATCHPAR_end_index"]
    #mysqldb_config = os.environ["BATCHPAR_config"]
    es_host = os.environ["BATCHPAR_outinfo"]
    es_port = os.environ["BATCHPAR_out_port"]
    es_index = os.environ["BATCHPAR_out_index"]
    es_type = os.environ["BATCHPAR_out_type"]
    db = os.environ["BATCHPAR_db"]

    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db)
    Session = sessionmaker(bind=engine)
    session = Session()

    cols = ["application_id",
            "full_project_num",
            "fy",
            "org_city",
            "org_country",
            "org_state",
            "org_zipcode",
            "org_name",
            "project_start",
            "project_end",
            "project_terms",
            "project_title",
            "total_cost",
            "phr"
            ]
    cols_attrs = [getattr(Projects, c) for c in cols]
    batch_selection = session.query(*cols_attrs).filter(
            Projects.application_id >= start_index,
            Projects.application_id <= end_index).selectable
    df = pd.read_sql(batch_selection, session.bind)
    df.columns = [c[13::] for c in df.columns]  # remove the 'nih_projects_' prefix

    # geocode the dataframe
    df = df.rename(columns={'org_city': 'city', 'org_country': 'country'})
    df = geocode_dataframe(df)

    # append iso codes for country
    df = country_iso_code_dataframe(df)

    # clean start and end dates
    for col in ["project_start", "project_end"]:
        df[col] = df[col].apply(_extract_date)

    # apply schema
    df = schema_transformer(df, filename="nih.json",
                            from_key='tier_0', to_key='tier_1',
                            ignore=['application_id'])

    # output to elasticsearch
    es = Elasticsearch(es_host, port=es_port, sniff_on_start=True)
    # TODO: implement https
    # , scheme='https')

    for _, row in df.iterrows():
        doc = dict(row.loc[~pd.isnull(row)])
        uid = doc.pop("application_id")
        es.index(es_index, doc_type=es_type, id=uid, body=doc)


if __name__ == '__main__':
    run()
