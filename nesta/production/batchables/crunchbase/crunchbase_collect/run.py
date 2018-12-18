from ast import literal_eval
import boto3
import logging
import os
# import pandas as pd
from urllib.parse import urlsplit

from nesta.packages.crunchbase.crunchbase_collect import get_files_from_tar, process_non_orgs
from nesta.packages.crunchbase.crunchbase_collect import _insert_data
from nesta.production.orms.orm_utils import get_mysql_engine, try_until_allowed
from nesta.production.orms.orm_utils import get_class_by_tablename, db_session
from nesta.production.orms.crunchbase_orm import Base


def parse_s3_path(path):
    '''For a given S3 path, return the bucket and key values'''
    parsed_path = urlsplit(path)
    s3_bucket = parsed_path.netloc
    s3_key = parsed_path.path.lstrip('/')
    return (s3_bucket, s3_key)


def run():
    test = literal_eval(os.environ["BATCHPAR_test"])
    db_config = os.environ["BATCHPAR_config"]
    db_name = os.environ["BATCHPAR_db_name"]
    table = os.environ["BATCHPAR_table"]
    batch_size = int(os.environ["BATCHPAR_batch_size"])
    s3_path = os.environ["BATCHPAR_outinfo"]

    logging.info(f"Processing {table} file")

    # database setup
    engine = get_mysql_engine(db_config, "mysqldb", db_name)
    try_until_allowed(Base.metadata.create_all, engine)
    table_name = f"crunchbase_{table}"
    table_class = get_class_by_tablename(Base, table_name)

    # collect file
    df = get_files_from_tar(table, test=test)[0]

    # get primary keys and set of all existing in the db
    pk_cols = list(table_class.__table__.primary_key.columns)
    pk_names = [pk.name for pk in pk_cols]
    with db_session(engine) as session:
        existing_rows = set(session.query(*pk_cols).all())

    # process and insert data
    processed_rows = process_non_orgs(df, existing_rows, pk_names)
    _insert_data(db_config, 'mysqldb', db_name, Base, table_class,
                 processed_rows, batch_size=batch_size)

    # Mark the task as done
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(*parse_s3_path(s3_path))
    s3_obj.put(Body="")


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")
    run()

