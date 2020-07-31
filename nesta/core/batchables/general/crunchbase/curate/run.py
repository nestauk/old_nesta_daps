"""
run.py (general.crunchbase.curate)
=================================

Curate crunchbase data, ready for ingestion to the general ES endpoint.
"""

from nesta.core.luigihacks.elasticsearchplus import ElasticsearchPlus
from nesta.core.luigihacks.elasticsearchplus import _null_empty_str, __floatify_coord
from nesta.core.luigihacks.elasticsearchplus import _clean_up_lists, _remove_padding
from nesta.core.luigihacks.elasticsearchplus import _country_detection

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
    """Pop a value from dict by key, then convert to float if not None.

    Args:
        d (dict): dict to remove key k from.
        k: Key to pop from dict d.
    Returns:
        v: Value popped from dict d, convert to float if not None.
    """
    v = d.pop(k)
    if v is not None:
        return float(v)
    return v


def reformat_row(row, investor_names, categories, categories_groups_list):
    """Curate raw data for ingestion to MySQL.

    Args:
        row (dict): Row of data.
        investor_names (list): List of investor names for this org.
        categories (list): List of crunchbase categories for this org.
        categories_groups_list (list): List of crunchbase category groups for this org
    Returns:
        row (dict): Reformatted row of data
    """
    states_lookup = get_us_states_lookup()  # Note: this is lru_cached
    continent_lookup = get_continent_lookup()  # Note: this is lru_cached
    eu_countries = get_eu_countries()  # Note: this is lru_cached
    row['aliases'] = [row.pop('legal_name')] + [row.pop(f'alias{i}') for i in [1, 2, 3]]
    row['aliases'] = sorted(set(a for a in row['aliases'] if a is not None))
    row['investor_names'] = sorted(set(investor_names))
    row['is_eu'] = row['country_alpha_2'] in eu_countries
    row['coordinates'] = {'lat': row.pop('latitude'), 'lon': row.pop('longitude')}
    row['updated_at'] = row['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
    row['category_list'] = sorted(set(categories))
    row['category_groups_list'] = sorted(set(categories_groups_list))
    row['state_name'] = states_lookup[row['state_code']]
    row['continent_name'] = continent_lookup[row['continent']]


    row['coordinates'] = __floatify_coord(row['coordinates'])
    row = _country_detection(row, 'country_mentions')
    row = _remove_padding(row)
    row = _null_empty_str(row)
    row = _clean_up_lists(row)
    return row


def sqlalchemy_to_dict(_row, org_fields, geo_fields):
    """Pull relevant fields out of the JOIN'd org-geo object.

    Args:
        _row (sqlalchemy model): JOIN'd org-geo object.
        org_fields (list): List of Organization fields to extract.
        geo_fields (list): List of Geographic fields to extract.
    returns:
        row (dict): dict representation of the sqlalchemy model, with
                    releveant fields extracted.
    """
    row = {}
    row.update({k: v for k, v in _row.Organization.__dict__.items()
                if k in org_fields})
    row.update({k: v for k, v in _row.Geographic.__dict__.items()
                if k in geo_fields})
    return row


def retrieve_categories(_row, session):
    """Retrieve Crunchbase categories for this Organization

    Args:
        _row (sqlalchemy model): JOIN'd org-geo object.
        session (sqlalchemy connectable): SqlAlchemy connectable
    returns:
        row (dict): dict representation of the sqlalchemy model, with
                    releveant fields extracted.
    """
    categories, groups_list = [], []
    for category in (session.query(CategoryGroup)
                     .select_from(OrganizationCategory)
                     .join(CategoryGroup)
                     .filter(OrganizationCategory.organization_id==_row.Organization.id)
                     .all()):
        categories.append(category.name)
        groups_list += [group for group in str(category.category_groups_list).split('|')
                        if group != 'None']
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
            # Pop fields which aren't required
            to_pop = [k for k in row if k not in CrunchbaseOrg.__dict__]
            for k in to_pop:
                row.pop(k)
            # Append the row for bulk insertion
            data.append(row)
        insert_data("MYSQLDB", "mysqldb", db_name, Base,
                    CrunchbaseOrg, data, low_memory=True)
    logging.info("Batch job complete.")


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")
    run()
