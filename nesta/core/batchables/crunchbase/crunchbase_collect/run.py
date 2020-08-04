"""
run.py (crunchbase_collect)
===========================

Collect Crunchbase data from the proprietary data dump and pipe into the MySQL database.
"""

from ast import literal_eval
import boto3
import logging
import os
from urllib.parse import urlsplit

from nesta.packages.crunchbase.crunchbase_collect import get_files_from_tar, process_non_orgs
from nesta.core.orms.orm_utils import insert_data
from nesta.packages.misc_utils.batches import split_batches
from nesta.core.orms.orm_utils import get_mysql_engine, try_until_allowed
from nesta.core.orms.orm_utils import get_class_by_tablename, db_session
from nesta.core.orms.crunchbase_orm import Base
from nesta.core.luigihacks.s3 import parse_s3_path


def run():
    test = literal_eval(os.environ["BATCHPAR_test"])
    db_name = os.environ["BATCHPAR_db_name"]
    table = os.environ["BATCHPAR_table"]
    batch_size = int(os.environ["BATCHPAR_batch_size"])
    s3_path = os.environ["BATCHPAR_outinfo"]

    logging.warning(f"Processing {table} file")

    # database setup
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db_name)
    try_until_allowed(Base.metadata.create_all, engine)
    table_name = f"crunchbase_{table}"
    table_class = get_class_by_tablename(Base, table_name)

    # collect file
    nrows = 1000 if test else None
    df = get_files_from_tar([table], nrows=nrows)[0]
    logging.warning(f"{len(df)} rows in file")

    # get primary key fields and set of all those already existing in the db
    pk_cols = list(table_class.__table__.primary_key.columns)
    pk_names = [pk.name for pk in pk_cols]
    with db_session(engine) as session:
        existing_rows = set(session.query(*pk_cols).all())

    # process and insert data
    processed_rows = process_non_orgs(df, existing_rows, pk_names)
    for batch in split_batches(processed_rows, batch_size):
        insert_data("BATCHPAR_config", 'mysqldb', db_name, Base, table_class,
                    processed_rows, low_memory=True)

    logging.warning(f"Marking task as done to {s3_path}")
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(*parse_s3_path(s3_path))
    s3_obj.put(Body="")

    logging.warning("Batch job complete.")


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")

    os.environ["BATCHPAR_outinfo"] = "s3://nesta-production-intermediate/jobs_production"
    os.environ["BATCHPAR_config"] = "/home/ec2-user/nesta/nesta/core/config/mysqldb.config"
    os.environ["BATCHPAR_batch_size"] = "500"
    os.environ["BATCHPAR_table"] = "jobs"
    os.environ["BATCHPAR_test"] = "False"
    os.environ["BATCHPAR_db_name"] = "production"
    run()
