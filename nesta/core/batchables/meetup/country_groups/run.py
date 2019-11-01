"""
run.py (country_groups)
-----------------------

Batchable for expanding from countries to groups
"""

import logging
import os
from ast import literal_eval
import boto3
from urllib.parse import urlsplit

from nesta.packages.meetup.country_groups import MeetupCountryGroups
from nesta.packages.meetup.meetup_utils import flatten_data
from nesta.core.orms.orm_utils import insert_data
from nesta.core.orms.meetup_orm import Base
from nesta.core.orms.meetup_orm import Group

from sqlalchemy.orm import sessionmaker
from sqlalchemy import func


def parse_s3_path(path):
    '''For a given S3 path, return the bucket and key values'''
    parsed_path = urlsplit(path)
    s3_bucket = parsed_path.netloc
    s3_key = parsed_path.path.lstrip('/')
    return (s3_bucket, s3_key)


def run():
    logging.getLogger().setLevel(logging.INFO)

    # Fetch the input parameters
    iso2 = os.environ["BATCHPAR_iso2"]
    name = os.environ["BATCHPAR_name"]
    category = os.environ["BATCHPAR_cat"]
    coords = literal_eval(os.environ["BATCHPAR_coords"])
    radius = float(os.environ["BATCHPAR_radius"])
    s3_path = os.environ["BATCHPAR_outinfo"]
    db = os.environ["BATCHPAR_db"]
    
    # Get the data
    mcg = MeetupCountryGroups(country_code=iso2, 
                              category=category,
                              coords=coords,
                              radius=radius)
    mcg.get_groups_recursive()
    output = flatten_data(mcg.groups,
                          country_name=name,
                          country=iso2,
                          timestamp=func.utc_timestamp(),
                          keys=[('category', 'name'),
                                ('category', 'shortname'),
                                ('category', 'id'),
                                'description',
                                'created',
                                'country',
                                'city',
                                'id',
                                'lat',
                                'lon',
                                'members',
                                'name',
                                'topics',
                                'urlname'])

    # Add the data
    objs = insert_data("BATCHPAR_config", "mysqldb", db,
                       Base, Group, output)

    # Mark the task as done
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(*parse_s3_path(s3_path))
    s3_obj.put(Body="")

    # Mainly for testing
    return len(objs)


if __name__ == "__main__":
    run()
