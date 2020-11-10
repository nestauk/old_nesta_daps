# TODO: Check NULL core_project_nums

"""
run.py (general.nih.curate)
===========================

Curate NiH data, ready for ingestion to the general ES endpoint.
"""

from nesta.core.luigihacks.elasticsearchplus import _null_empty_str
from nesta.core.luigihacks.elasticsearchplus import __floatify_coord
from nesta.core.luigihacks.elasticsearchplus import _clean_up_lists
from nesta.core.luigihacks.elasticsearchplus import _remove_padding
from nesta.core.luigihacks.elasticsearchplus import _country_detection

from ast import literal_eval
import boto3
import json
import logging
import os
import pandas as pd
import requests
from collections import defaultdict

from nesta.packages.geo_utils.lookup import get_us_states_lookup
from nesta.packages.geo_utils.lookup import get_continent_lookup
from nesta.packages.geo_utils.lookup import get_eu_countries

from nesta.core.orms.orm_utils import db_session, get_mysql_engine
from nesta.core.orms.orm_utils import load_json_from_pathstub, insert_data

from nesta.core.orms.orm_utils import object_to_dict
from itertools import groupby
from operator import attrgetter


# Input ORMs:
from nesta.core.orms.nih_orm import Projects, Abstracts
from nesta.core.orms.nih_orm import TextDuplicate

# Output ORM
from nesta.core.orms.general_orm import NihProject, Base

def group_projects_by_core_id(engine, core_ids, nrows=None):
    if core_ids is not None:
        filter_stmt = ((Projects.core_project_num != None) & \
                       (Projects.core_project_num.in_(core_ids)))
    else:
        filter_stmt = (Projects.core_project_num == None)

    with db_session(engine) as session:
        q = (session.query(Projects).filter(filter_stmt)
             .order_by(Projects.core_project_num))
        results = q.limit(nrows).all()
        groups = [[object_to_dict(obj) for obj in group] for _, group in
                  groupby(results, attrgetter('core_project_num'))]
    return groups


def retrieve_duplicates(engine, appl_ids, duplicate_threshold=0.8): 
    # Retrieve all duplicate projects, which will include an overlap
    # with appl_ids if there are already projects in this group
    filter_stmt = ((TextDuplicate.application_id_1.in_(appl_ids) | 
                    TextDuplicate.application_id_2.in_(appl_ids)) &  
                    TextDuplicate.weight >= duplicate_threshold) 
    with db_session(engine) as session: 
        dupes = session.query(TextDuplicate).filter(filter_stmt).all() 
        dupes = [object_to_dict(obj) for obj in dupes]

    # Subset the new IDs, so we don't recollect any known duplicates
    # in this group
    dupe_ids = set([d['application_id_1'] for d in dupes] +\ 
                   [d['application_id_2'] for d in dupes])
    dupe_ids = dupe_ids - appl_ids

    # Collect the new duplicates
    filter_stmt = Projects.application_id.in_(dupe_ids)
    with db_session(engine) as session: 
        new_projs = session.query(Projects).filter(filter_stmt)
        new_projs = [object_to_dict(obj) for obj in dupe_projs]
    return new_projs 



def assign_duplicates(row):
    """For this row"""

def reformat_row(row):
    """Curate raw data for ingestion to MySQL.

    Args:
        row (dict): Row of data.
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


def run():
    test = literal_eval(os.environ["BATCHPAR_test"])
    bucket = os.environ['BATCHPAR_bucket']
    batch_file = os.environ['BATCHPAR_batch_file']
    db_name = os.environ["BATCHPAR_db_name"]
    os.environ["MYSQLDB"] = os.environ["BATCHPAR_config"]

    # Database setup
    engine = get_mysql_engine("MYSQLDB", "mysqldb", db_name)

    # Retrieve list of Org ids from S3
    nrows = 1000 if test else None
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, batch_file)
    core_ids = json.loads(obj.get()['Body']._raw_stream.read())
    logging.info(f"{len(core_ids)} projects retrieved from s3")

    data = []    
    for group in group_projects_by_core_id(engine, core_ids):
        row = assign_duplicates(group)
        if len(row) == 0:
            continue
        row = concat_group(group)
        row = reformat_row(row)
        data.append(row)

    insert_data("MYSQLDB", "mysqldb", db_name, Base,
                NihProject, data, low_memory=True)
    logging.info("Batch job complete.")


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")
    run()
