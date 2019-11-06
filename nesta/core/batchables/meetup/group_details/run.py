"""
run.py (group_details)
----------------------

Batchable for expanding group details
"""

import logging
from nesta.packages.meetup.group_details import get_group_details
from nesta.packages.meetup.meetup_utils import flatten_data
from nesta.core.orms.orm_utils import insert_data
from nesta.core.orms.meetup_orm import Base
from nesta.core.orms.meetup_orm import Group
from sqlalchemy.orm import sessionmaker
import boto3
from urllib.parse import urlsplit
import os
from ast import literal_eval
from nesta.core.luigihacks.s3 import parse_s3_path


def run():
    logging.getLogger().setLevel(logging.INFO)

    # Fetch the input parameters
    group_urlnames = literal_eval(os.environ["BATCHPAR_group_urlnames"])
    group_urlnames = [x.decode("utf8") for x in group_urlnames]
    s3_path = os.environ["BATCHPAR_outinfo"]
    db = os.environ["BATCHPAR_db"]

    # Generate the groups for these members
    _output = []
    for urlname in group_urlnames:
        _info = get_group_details(urlname, max_results=200)
        if len(_info) == 0:
            continue
        _output.append(_info)
    logging.info("Processed %s groups", len(_output))

    # Flatten the output
    output = flatten_data(_output,
                          keys = [('category', 'name'),
                                  ('category', 'shortname'),
                                  ('category', 'id'),
                                  'created',
                                  'country',
                                  'city',
                                  'description',
                                  'id',
                                  'lat',
                                  'lon',
                                  'members',
                                  'name',
                                  'topics',
                                  'urlname'])

    objs = insert_data("BATCHPAR_config", "mysqldb", db,
                       Base, Group, output[48:49])

    # Mark the task as done
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(*parse_s3_path(s3_path))
    s3_obj.put(Body="")

    return len(objs)


if __name__ == "__main__":
    run()
