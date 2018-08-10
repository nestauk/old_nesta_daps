import logging
from meetup.groups_members import get_all_members
from orms.orm_utils import get_mysql_engine
from orms.orm_utils import try_until_allowed
from orms.meetup_orm import Base
from orms.meetup_orm import GroupMember
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

    # Collect members
    logging.info("Getting %s", group_urlname)
    output = get_all_members(group_id, group_urlname, max_results=200)
    logging.info("Got %s members", len(output))

    # Load connection to the db, and create the tables
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", "production")
    try_until_allowed(Base.metadata.create_all, engine)
    Session = try_until_allowed(sessionmaker, engine)
    session = try_until_allowed(Session)

    # Add the data
    for row in output:
        and_stmt = and_(GroupMember.group_id == row["group_id"],
                        GroupMember.member_id == row["member_id"])
        q = session.query(GroupMember).filter(and_stmt)
        if q.count() > 0:
            continue
        g = GroupMember(**row)
        session.merge(g)
    
    session.commit()
    session.close()

    # Mark the task as done
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(*parse_s3_path(s3_path))
    s3_obj.put(Body="")


if __name__ == "__main__":
    run()
