from ast import literal_eval
import boto3
from nesta.production.luigihacks.elasticsearch import ElasticsearchPlus
import json
import logging
import os
import pandas as pd

from nesta.production.orms.orm_utils import db_session, get_mysql_engine
from nesta.production.orms.orm_utils import load_json_from_pathstub
from nesta.production.orms.crunchbase_orm import Organization, OrganizationCategory, CategoryGroup
from nesta.production.orms.geographic_orm import Geographic


def run():
    test = literal_eval(os.environ["BATCHPAR_test"])
    bucket = os.environ['BATCHPAR_bucket']
    batch_file = os.environ['BATCHPAR_batch_file']

    db_name = os.environ["BATCHPAR_db_name"]
    es_host = os.environ['BATCHPAR_outinfo']
    es_port = os.environ['BATCHPAR_out_port']
    es_index = os.environ['BATCHPAR_out_index']
    es_type = os.environ['BATCHPAR_out_type']
    entity_type = os.environ["BATCHPAR_entity_type"]

    # database setup
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db_name)
    static_engine = get_mysql_engine("BATCHPAR_config", "mysqldb", "static_data")
    states_lookup = {row['state_code']: row['state_name'] 
                     for _, row in  pd.read_sql_table('us_states_lookup', 
                                                      static_engine).iterrows()}
    states_lookup[None] = None  # default lookup for non-US countries

    # es setup
    field_null_mapping = load_json_from_pathstub("tier_1/field_null_mapping/",
                                                 "health_scanner.json")
    strans_kwargs={'filename':'crunchbase_organisation_members.json',
                   'from_key':'tier_0',
                   'to_key':'tier_1',
                   'ignore':['id']}
    es = ElasticsearchPlus(hosts=es_host, 
                           port=es_port, 
                           use_ssl=True,
                           entity_type=entity_type,
                           strans_kwargs=strans_kwargs,
                           field_null_mapping=field_null_mapping,
                           null_empty_str=True,
                           coordinates_as_floats=True,
                           country_detection=True,
                           listify_terms=True)

    # collect file
    nrows = 1000 if test else None

    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, batch_file)
    org_ids = json.loads(obj.get()['Body']._raw_stream.read())
    logging.info(f"{len(org_ids)} organisations retrieved from s3")

    geo_fields = ['country_alpha_2', 'country_alpha_3', 'country_numeric', 
                  'continent', 'latitude', 'longitude']
    with db_session(engine) as session:
        rows = (session
                .query(Organization, Geographic)
                .join(Geographic, Organization.location_id==Geographic.id)
                .filter(Organization.id.in_(org_ids))
                .limit(nrows)
                .all())
        for count, row in enumerate(rows, 1):
            # convert sqlalchemy to dictionary
            row_combined = {k: v for k, v in row.Organization.__dict__.items()}
            row_combined['currency_of_funding'] = 'USD'  # all values are from 'funding_total_usd'

            row_combined.update({k: v for k, v in row.Geographic.__dict__.items() 
                                 if k in geo_fields})

            # reformat coordinates
            row_combined['coordinates'] = {'lat': row_combined.pop('latitude'),
                                           'lon': row_combined.pop('longitude')}

            # iterate through categories and groups
            row_combined['category_list'] = []
            row_combined['category_group_list'] = []
            for category in (session.query(CategoryGroup)
                             .select_from(OrganizationCategory)
                             .join(CategoryGroup)
                             .filter(OrganizationCategory.organization_id==row.Organization.id)
                             .all()):
                row_combined['category_list'].append(category.category_name)
                row_combined['category_group_list'] += [group for group
                                                        in str(category.category_group_list).split('|')
                                                        if group is not 'None']

            # Add a field for US state name
            state_code = row_combined['id_state_organization']
            row_combined['placeName_state_organization'] = states_lookup[state_code]
            uid = row_combined.pop('id')
            es.index(index=es_index, doc_type=es_type, 
                     id=uid, body=row_combined)

            if not count % 1000:
                logging.info(f"{count} rows loaded to elasticsearch")

    logging.warning("Batch job complete.")


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")
    run()
