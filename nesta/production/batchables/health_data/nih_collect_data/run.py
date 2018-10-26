from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_

from nesta.production.orms.orm_utils import get_mysql_engine
from nesta.production.orms.orm_utils import get_class_by_tablename
from nesta.production.orms.orm_utils import try_until_allowed
from nesta.production.orms.orm_utils import exists

from nesta.production.orms.nih_orm import Base

from nesta.packages.health_data.collect_nih import get_data_urls
from nesta.packages.health_data.collect_nih import iterrows
from nesta.packages.health_data.process_nih import geocode_dataframe
from nesta.packages.health_data.process_nih import _extract_date

import os
import boto3
from urllib.parse import urlsplit


def parse_s3_path(path):
    '''For a given S3 path, return the bucket and key values'''
    parsed_path = urlsplit(path)
    s3_bucket = parsed_path.netloc
    s3_key = parsed_path.path.lstrip('/')
    return (s3_bucket, s3_key)


def run():
    table_name = os.environ["BATCHPAR_table_name"]
    url = os.environ["BATCHPAR_url"]
    db_name = os.environ["BATCHPAR_db_name"]
    s3_path = os.environ["BATCHPAR_outinfo"]

    # Setup the database connectors
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db_name)
    try_until_allowed(Base.metadata.create_all, engine)
    _class = get_class_by_tablename(Base, table_name)
    Session = try_until_allowed(sessionmaker, engine)
    session = try_until_allowed(Session)
                 
    # Commit the data
    all_pks = set()
    objs = []
    pkey_cols = _class.__table__.primary_key.columns
    for row in iterrows(url):
        if len(row) == 0:
            continue
        if session.query(exists(_class, **row)).scalar():
            continue
        pk = tuple([row[pkey.name] for pkey in pkey_cols])
        if pk in all_pks:
            continue
        all_pks.add(pk)
        objs.append(_class(**row))
    session.bulk_save_objects(objs)
    session.commit()
    session.close()

    # Mark the task as done 
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(*parse_s3_path(s3_path))
    s3_obj.put(Body="")

if __name__ == "__main__":
    run()
