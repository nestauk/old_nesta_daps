import os
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import logging

from nesta.core.orms.orm_utils import load_json_from_pathstub
from nesta.core.luigihacks.elasticsearchplus import ElasticsearchPlus

from nesta.packages.health_data.process_nih import _extract_date
from nesta.packages.geo_utils.geocode import geocode_dataframe
from nesta.packages.geo_utils.lookup import get_continent_lookup
from nesta.packages.geo_utils.country_iso_code import country_iso_code_dataframe
from nesta.core.orms.orm_utils import get_mysql_engine
from nesta.core.orms.nih_orm import Projects


def run():
    start_index = os.environ["BATCHPAR_start_index"]
    end_index = os.environ["BATCHPAR_end_index"]
    #mysqldb_config = os.environ["BATCHPAR_config"]
    es_host = os.environ["BATCHPAR_outinfo"]
    es_port = os.environ["BATCHPAR_out_port"]
    es_index = os.environ["BATCHPAR_out_index"]
    es_type = os.environ["BATCHPAR_out_type"]
    entity_type = os.environ["BATCHPAR_entity_type"]
    db = os.environ["BATCHPAR_db"]
    aws_auth_region = os.environ["BATCHPAR_aws_auth_region"]

    # Read in the US states
    static_engine = get_mysql_engine("BATCHPAR_config", "mysqldb", "static_data")
    states_lookup = {row['state_code']: row['state_name']
                     for _, row in  pd.read_sql_table('us_states_lookup',
                                                      static_engine).iterrows()}
    states_lookup[None] = None
    states_lookup[''] = None

    # Get continent lookup
    continent_lookup = get_continent_lookup()

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
            "phr",
            "ic_name"
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
        df[col] = df[col].apply(lambda x: _extract_date(x))

    # currency is the same for the whole dataset
    df['total_cost_currency'] = 'USD'

    # output to elasticsearch
    field_null_mapping = load_json_from_pathstub("tier_1/field_null_mappings/",
                                                 "health_scanner.json")
    strans_kwargs={'filename':'nih.json',
                   'from_key':'tier_0',
                   'to_key':'tier_1',
                   'ignore':['application_id']}

    es = ElasticsearchPlus(hosts=es_host,
                           port=es_port,
                           aws_auth_region=aws_auth_region,
                           no_commit=("AWSBATCHTEST" in os.environ),
                           entity_type=entity_type,
                           strans_kwargs=strans_kwargs,
                           field_null_mapping=field_null_mapping,
                           null_empty_str=True,
                           coordinates_as_floats=True,
                           country_detection=True,
                           listify_terms=True,
                           terms_delimiters=(";",","),
                           caps_to_camel_case=True,
                           null_pairs={"currency_total_cost": "cost_total_project"})

    for _, row in df.iterrows():
        doc = dict(row.loc[~pd.isnull(row)])
        if 'country' in doc:
            # Try to patch broken US data
            if doc['country'] == '' and doc['org_state'] != '':
                doc['country'] = "United States"
                doc['continent'] = "NA"
            doc['placeName_state_organisation'] = states_lookup[doc['org_state']]

            if 'continent' in doc:
                continent_code = doc['continent']
            else:
                continent_code = None
            doc['placeName_continent_organisation'] = continent_lookup[continent_code]

        if 'ic_name'in doc:
            doc['ic_name'] = [doc['ic_name']]

        uid = doc.pop("application_id")
        es.index(index=es_index,
                 doc_type=es_type, id=uid, body=doc)


if __name__ == '__main__':
    log_level = logging.INFO
    if 'BATCHPAR_outinfo' not in os.environ:
        logging.getLogger('boto3').setLevel(logging.CRITICAL)
        logging.getLogger('botocore').setLevel(logging.CRITICAL)
        logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
        logging.getLogger('urllib3').setLevel(logging.CRITICAL)
        log_level = logging.INFO
        pars = {'start_index': '2001360',
                'end_index': '2003940',
                'db': 'dev',
                'config': (f'{os.environ["HOME"]}/nesta/nesta/'
                           'core/config/mysqldb.config'),
                'done': 'False',
                'outinfo': ('https://search-health-scanner-'
                            '5cs7g52446h7qscocqmiky5dn4.'
                            'eu-west-2.es.amazonaws.com'),
                'out_index': 'nih_dev',
                'out_type': '_doc',
                'out_port': '_doc',
                'aws_auth_region': 'eu-west-2',
                'entity_type': 'paper',
                'test': 'False'}
        for k, v in pars.items():
            os.environ[f'BATCHPAR_{k}'] = v

    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=log_level,
                        format="%(asctime)s:%(levelname)s:%(message)s")
    run()
