from ast import literal_eval
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
import json
import logging
import os
from requests_aws4auth import AWS4Auth

from nesta.packages.decorators.schema_transform import schema_transformer
from nesta.production.orms.orm_utils import db_session, get_mysql_engine
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

    # database setup
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db_name)

    # elasticsearch setup
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key,
                       'eu-west-2', 'es')
    es = Elasticsearch(
        hosts=[{'host': es_host, 'port': es_port}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    # retrieve batch org_ids from s3
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, batch_file)
    org_ids = json.loads(obj.get()['Body']._raw_stream.read())
    logging.info(f"{len(org_ids)} organisations retrieved from s3")

    geo_fields = ['country_alpha_2', 'country_alpha_3', 'country_numeric',
                  'continent', 'latitude', 'longitude']
    nrows = 1000 if test else None

    # collect orgs and geo data from database
    with db_session(engine) as session:
        rows = (session
                .query(Organization, Geographic)
                .join(Geographic, Organization.location_id == Geographic.id)
                .filter(Organization.id.in_(org_ids))
                .limit(nrows)
                .all())
        for count, row in enumerate(rows, 1):
            # convert sqlalchemy to dict
            row_combined = {k: v for k, v in row.Organization.__dict__.items()}
            row_combined.update({k: v for k, v in row.Geographic.__dict__.items()
                                 if k in geo_fields})

            # reformatting
            row_combined['coordinates'] = {'lat': row_combined.pop('latitude'),
                                           'lon': row_combined.pop('longitude')}
            row_combined['mesh_terms'] = row_combined['mesh_terms'].split('|')
            row_combined['currency_of_funding'] = 'USD'  # all from 'funding_total_usd'

            # extract categories and category groups
            row_combined['category_list'] = []
            row_combined['category_group_list'] = []
            for category in (session
                             .query(CategoryGroup)
                             .select_from(OrganizationCategory)
                             .join(CategoryGroup)
                             .filter(OrganizationCategory.organization_id == row.Organization.id)
                             .all()):
                row_combined['category_list'].append(category.category_name)
                if category.category_group_list is not None:
                    row_combined['category_group_list'] += category.category_group_list.split('|')

            # schema transform and write to elasticsearch
            uid = row_combined.pop('id')
            row_combined = schema_transformer(row_combined,
                                              filename='crunchbase_organisation_members.json',
                                              from_key='tier_0',
                                              to_key='tier_1')
            es.index(es_index, doc_type=es_type, id=uid, body=row_combined)
            if not count % 1000:
                logging.info(f"{count} rows loaded to elasticsearch")

    logging.warning("Batch job complete")


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")
    run()
