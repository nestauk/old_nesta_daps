import pandas as pd
import os
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from sqlalchemy.orm import sessionmaker

from nesta.packages.health_data.process_nih import _extract_date
from nesta.packages.health_data.process_nih import geocode_dataframe
from nesta.packages.health_data.process_nih import country_iso_code_dataframe
from nesta.packages.decorators.schema_transform import schema_transform
from nesta.production.orms.orm_utils import get_mysql_engine
from nesta.production.orms.world_reporter_orm import Projects



# @schema_transform(mapping, from_key, to_key)
def run():
    start_index = os.environ["BATCHPAR_start_index"]
    end_index = os.environ["BATCHPAR_end_index"]
    mysqldb_config = os.environ["BATCHPAR_config"]
    outpath = os.environ["BATCHPAR_outinfo"]
    # variable for prod/dev

    engine = get_mysql_engine(mysqldb_config, "mysqldb", "dev")  # replace with mysqlconfig
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
            "total_cost"
            ]
    cols_attrs = [getattr(Projects, c) for c in cols]
    batch_selection = session.query(*cols_attrs).filter(
            Projects.application_id >= start_index,
            Projects.application_id <= end_index).selectable
    df = pd.read_sql(batch_selection, session.bind)
    df.columns = [c[13::] for c in df.columns]  # remove the 'nih_projects_' prefix

    df = df.rename(columns={'org_city': 'city', 'org_country': 'country'})


    # Geocode the dataframe
    import pdb; pdb.set_trace()
    df = geocode_dataframe(df)
    # clean start and end dates
    for col in ["project_start", "project_end"]:
        df[col] = df[col].apply(_extract_date)
    # append iso codes for country
    df = country_iso_code_dataframe(df)

    print(df)



    # es = Elasticsearch([os.environ["BATCHPAR_outinfo"]], 
    #                    port=443, scheme="https")
    # for _, row in df.iterrows():
    #     doc = dict(row.loc[~pd.isnull(row)])
    #     doc.pop("unnamed:_0")
    #     uid = doc.pop("unique_id")
    #     res = es.index(index='rwjf_uid', doc_type='world_reporter', 
    #                    id=uid, body=doc)



if __name__ == "__main__":
    os.environ["BATCHPAR_start_index"] = '100001'
    os.environ["BATCHPAR_end_index"] = '100009'
    os.environ["BATCHPAR_config"] = 'MYSQLDB'
    os.environ["BATCHPAR_outinfo"] = 'testing'
    run()
