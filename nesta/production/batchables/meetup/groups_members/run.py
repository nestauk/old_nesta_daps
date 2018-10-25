import logging
from nesta.packages.meetup.groups_members import get_all_members
from nesta.production.orms.orm_utils import insert_data
from nesta.production.orms.meetup_orm import Base
from nesta.production.orms.meetup_orm import GroupMember
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
import boto3
from urllib.parse import urlsplit
import os


def parse_s3_path(path):
    '''For a given S3 path, return the bucket and key values'''
    parsed_path = urlsplit(path)
    s3_bucket = parsed_path.netloc
    s3_key = parsed_path.path.lstrip('/')
    return (s3_bucket, s3_key)


def run():
    logging.getLogger().setLevel(logging.INFO)
    
    # Fetch the input parameters
    group_urlname = os.environ["BATCHPAR_group_urlname"]
    group_id = os.environ["BATCHPAR_group_id"]
    s3_path = os.environ["BATCHPAR_outinfo"]
    db = os.environ["BATCHPAR_db"]

    # Collect members
    logging.info("Getting %s", group_urlname)
    output = get_all_members(group_id, group_urlname, max_results=200)
    logging.info("Got %s members", len(output))

    # Load connection to the db, and create the tables
    objs = insert_data("BATCHPAR_config", "mysqldb", db, 
                       Base, GroupMember, output)
    # Mainly for testing
    return len(objs)


if __name__ == "__main__":
    run()
