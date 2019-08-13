from ast import literal_eval
import boto3
import logging
import os
from urllib.parse import urlsplit

from nesta.packages.examples.example_package import some_func  # example package
from nesta.production.orms.example_orm import Base, MyTable, MyOtherTable  # example orm
from nesta.production.orms.orm_utils import get_mysql_engine, try_until_allowed
from nesta.production.orms.orm_utils import insert_data, db_session


def parse_s3_path(path):
    '''For a given S3 path, return the bucket and key values'''
    parsed_path = urlsplit(path)
    s3_bucket = parsed_path.netloc
    s3_key = parsed_path.path.lstrip('/')
    return (s3_bucket, s3_key)


def run():
    test = literal_eval(os.environ["BATCHPAR_test"])
    db_name = os.environ["BATCHPAR_db_name"]
    batch_size = int(os.environ["BATCHPAR_batch_size"])  # example parameter
    s3_path = os.environ["BATCHPAR_outinfo"]
    start_string = os.environ["BATCHPAR_start_string"],  # example parameter
    offset = int(os.environ["BATCHPAR_offset"])

    # reduce records in test mode
    if test:
        limit = 50
        logging.info(f"Limiting to {limit} rows in test mode")
    else:
        limit = batch_size

    logging.info(f"Processing {offset} - {offset + limit}")

    # database setup
    logging.info(f"Using {db_name} database")
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db_name)
    try_until_allowed(Base.metadata.create_all, engine)

    with db_session(engine) as session:
        # consider moving this query and the one from the prepare step into a package
        batch_records = (session
                         .query(MyTable.id, MyTable.name)
                         .filter(MyTable.founded_on > '2007-01-01')
                         .offset(offset)
                         .limit(limit))

    # process and insert data
    processed_batch = []
    for row in batch_records:
        processed_row = some_func(start_string=start_string, row=row)
        processed_batch.append(processed_row)

    logging.info(f"Inserting {len(processed_batch)} rows")
    insert_data("BATCHPAR_config", 'mysqldb', db_name, Base, MyOtherTable,
                processed_batch, low_memory=True)

    logging.info(f"Marking task as done to {s3_path}")
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(*parse_s3_path(s3_path))
    s3_obj.put(Body="")

    logging.info("Batch job complete.")


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")
    run()
