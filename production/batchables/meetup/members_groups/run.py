import logging
from meetup.members_groups import get_member_details
from meetup.members_groups import get_member_groups
from orms.orm_utils import get_mysql_engine
from orms.orm_utils import try_until_allowed
from orms.meetup_orm import Base
from orms.meetup_orm import GroupMember
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
import boto3
from urllib.parse import urlsplit
import os
from ast import literal_eval
from sqlalchemy.exc import IntegrityError


def parse_s3_path(path):
    '''For a given S3 path, return the bucket and key values'''
    parsed_path = urlsplit(path)
    s3_bucket = parsed_path.netloc
    s3_key = parsed_path.path.lstrip('/')
    return (s3_bucket, s3_key)


def run():
    logging.getLogger().setLevel(logging.INFO)
    
    # Fetch the input parameters
    member_ids = literal_eval(os.environ["BATCHPAR_member_ids"])
    s3_path = os.environ["BATCHPAR_outinfo"]

    # Generate the groups for these members
    output = []
    for member_id in member_ids:
        response = get_member_details(member_id, max_results=200)
        output += get_member_groups(response)
    logging.info("Got %s groups", len(output))

    # Load connection to the db, and create the tables
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", "production")
    try_until_allowed(Base.metadata.create_all, engine)
    Session = try_until_allowed(sessionmaker, engine)
    session = try_until_allowed(Session)

    # Add the data
    for row in output:
        if 'group_id' not in row:
            continue
        and_stmt = and_(GroupMember.group_id == row["group_id"],
                        GroupMember.member_id == row["member_id"])
        #        q = session.query(GroupMember).filter(and_stmt)
        #        if q.count() > 0:
        #            continue
        g = GroupMember(**row)
        session.merge(g)
        session.flush()

    session.commit()
    session.close()

    # Mark the task as done
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(*parse_s3_path(s3_path))
    s3_obj.put(Body="")


if __name__ == "__main__":
    run()
