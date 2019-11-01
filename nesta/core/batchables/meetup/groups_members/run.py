"""
run.py (groups_members)
-------------------------------------------

Batchable for expanding group members
"""

import logging
from nesta.packages.meetup.groups_members import get_all_members
from nesta.core.orms.orm_utils import insert_data
from nesta.core.orms.meetup_orm import Base
from nesta.core.orms.meetup_orm import GroupMember
from nesta.core.luigihacks.s3 import parse_s3_path
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
import boto3
from urllib.parse import urlsplit
import os


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
