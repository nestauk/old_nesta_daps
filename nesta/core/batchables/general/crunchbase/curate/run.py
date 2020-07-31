"""
run.py (general.crunchbase.curate)
=================================

Curate crunchbase data, ready for ingestion to the general ES endpoint.
"""

from nesta.core.luigihacks.elasticsearchplus import ElasticsearchPlus

from ast import literal_eval
import boto3
import json
import logging
import os
import pandas as pd
import requests
from collections import defaultdict

from nesta.packages.crunchbase.utils import parse_investor_names
from nesta.packages.geo_utils.lookup import get_us_states_lookup
from nesta.packages.geo_utils.lookup import get_continent_lookup
from nesta.packages.geo_utils.lookup import get_eu_countries

from nesta.core.orms.orm_utils import db_session, get_mysql_engine
from nesta.core.orms.orm_utils import load_json_from_pathstub, insert_data

# Input ORMs:
from nesta.core.orms.crunchbase_orm import Organization
from nesta.core.orms.crunchbase_orm import OrganizationCategory
from nesta.core.orms.crunchbase_orm import CategoryGroup
from nesta.core.orms.crunchbase_orm import FundingRound
from nesta.core.orms.geographic_orm import Geographic

# Output ORM
from nesta.core.orms.general_orm import CrunchbaseOrg, Base

def float_pop(d, k):
    v = d.pop(k)
    if v is not None:
        return float(v)
    return v


def reformat_row(row, investor_names, categories, categories_groups_list):
    states_lookup = get_us_states_lookup()  # Note: this is lru_cached
    continent_lookup = get_continent_lookup()  # Note: this is lru_cached
    eu_countries = get_eu_countries()  # Note: this is lru_cached
    row['aliases'] = [row.pop('legal_name')] + [row.pop(f'alias{i}') for i in [1, 2, 3]]
    row['aliases'] = sorted(set(a for a in row['aliases'] if a is not None))
    row['currency_of_funding'] = 'USD'  # Only fall back on 'funding_total_usd'
    row['investor_names'] = sorted(set(investor_names))
    row['is_eu'] = row['country_alpha_2'] in eu_countries    
    row['coordinates'] = {'lat': float_pop(row, 'latitude'), 'lon': float_pop(row, 'longitude')}
    row['updated_at'] = row['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
    row['category_list'] = categories    
    row['category_group_list'] = categories_groups_list
    row['state_name'] = states_lookup[row['state_code']]
    row['continent_name'] = continent_lookup[row['continent']]
    return row


def sqlalchemy_to_dict(_row, org_fields, geo_fields):
    row = {}
    row.update({k: v for k, v in _row.Organization.__dict__.items()
                if k in org_fields})
    row.update({k: v for k, v in _row.Geographic.__dict__.items()
                if k in geo_fields})
    return row


def retrieve_categories(_row, session):
    categories, groups_list = [], []
    for category in (session.query(CategoryGroup)
                     .select_from(OrganizationCategory)
                     .join(CategoryGroup)
                     .filter(OrganizationCategory.organization_id==_row.Organization.id)
                     .all()):
        categories.append(category.name)
        groups_list += [group for group in str(category.category_groups_list).split('|')
                        if group is not 'None']
    return categories, groups_list

        
def run():
    test = literal_eval(os.environ["BATCHPAR_test"])
    bucket = os.environ['BATCHPAR_bucket']
    batch_file = os.environ['BATCHPAR_batch_file']
    db_name = os.environ["BATCHPAR_db_name"]
    os.environ["MYSQLDB"] = os.environ["BATCHPAR_config"]

    # Database setup
    engine = get_mysql_engine("MYSQLDB", "mysqldb", db_name)    
    # Retrieve lookup tables

    # Retrieve list of Org ids from S3
    nrows = 20 if test else None
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, batch_file)
    org_ids = json.loads(obj.get()['Body']._raw_stream.read())
    logging.info(f"{len(org_ids)} organisations retrieved from s3")
    # Lists of fields to extract
    org_fields = list(set(c.name for c in Organization.__table__.columns))
    geo_fields = ['country_alpha_2', 'country_alpha_3', 'country_numeric',
                  'continent', 'latitude', 'longitude']

    # First get all investors
    investor_names = defaultdict(list)
    with db_session(engine) as session:
        query = (session.query(Organization, FundingRound)
                 .join(FundingRound, Organization.id==FundingRound.org_id)
                 .filter(Organization.id.in_(org_ids)))
        for row in query.all():
            _investor_names = row.FundingRound.investor_names
            investor_names[row.Organization.id] += parse_investor_names(_investor_names)

    # Now process organisations
    with db_session(engine) as session:
        query = (session.query(Organization, Geographic)
                 .join(Geographic, Organization.location_id==Geographic.id)
                 .filter(Organization.id.in_(org_ids)))
        data = []
        for count, _row in enumerate(query.limit(nrows).all(), 1):
            row = sqlalchemy_to_dict(_row, org_fields=org_fields, geo_fields=geo_fields)
            categories, groups_list = retrieve_categories(_row, session)
            row = reformat_row(row, investor_names=investor_names[row['id']],
                               categories=categories, categories_groups_list=groups_list)
            to_pop = [k for k in row if k not in CrunchbaseOrg.__dict__]
            for k in to_pop:
                row.pop(k)
            data.append(row)
        insert_data("MYSQLDB", "mysqldb", db_name, Base,
                    CrunchbaseOrg, data, low_memory=True)
    logging.info("Batch job complete.")


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")
    if "BATCHPAR_test" not in os.environ:
        env = {"batch_file": "General-Curate-2020-07-29_crunchbase-15960376638858845.json",
               "config": os.environ["HOME"]+"/nesta/nesta/core/config/mysqldb.config",
               "routine_id": "General-Curate-2020-07-29_crunchbase",
               "bucket": "nesta-production-intermediate",
               "test": "True",
               "db_name": "dev"}
        for k, v in env.items():
            os.environ[f'BATCHPAR_{k}'] = v
    run()
