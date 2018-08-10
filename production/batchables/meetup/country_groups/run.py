import logging
from meetup.country_groups import MeetupCountryGroups
from meetup.meetup_utils import flatten_data
from orms.orm_utils import get_mysql_engine
from orms.orm_utils import try_until_allowed
from orms.meetup_orm import Base
from orms.meetup_orm import Group
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
import os
from ast import literal_eval
import boto3
from urllib.parse import urlsplit


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

    # Load connection to the db, and create the tables
    engine = get_mysql_engine("BATCHPAR_config",
                              "mysqldb", "production")
    try_until_allowed(Base.metadata.create_all, engine)
    Session = try_until_allowed(sessionmaker, engine)
    session = try_until_allowed(Session)
    
    # Add the data
    for row in output:
        q = session.query(Group).filter(Group.id == row["id"])
        if q.count() > 0:
            continue
        g = Group(**row)
        session.merge(g)        

    session.commit()
    session.close()

    # Mark the task as done
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(*parse_s3_path(s3_path))
    s3_obj.put(Body="")


if __name__ == "__main__":
    run()
