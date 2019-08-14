from ast import literal_eval
import boto3
import logging
import json
import os
from urllib.parse import urlsplit

# from nesta.packages.examples.example_package import some_func  # example package
from nesta.production.orms.gtr_orm import Projects  # example orm
from nesta.production.orms.orm_utils import get_mysql_engine  # , try_until_allowed
from nesta.production.orms.orm_utils import db_session
from nesta.packages.nlp_utils.text2vec import docs2vectors


def parse_s3_path(path):
    '''For a given S3 path, return the bucket and key values'''
    parsed_path = urlsplit(path)
    s3_bucket = parsed_path.netloc
    s3_key = parsed_path.path.lstrip('/')
    return (s3_bucket, s3_key)


def run():
    # test = literal_eval(os.environ["BATCHPAR_test"] )
    db_name = os.environ["BATCHPAR_db_name"]
    bucket = os.environ['BATCHPAR_bucket']  # fill this in
    batch_file = os.environ['BATCHPAR_batch_file']  # fill this in
    # batch_size = int(os.environ["BATCHPAR_batch_size"])  # example parameter
    # s3_path = os.environ["BATCHPAR_outinfo"]
    # start_string = os.environ["BATCHPAR_start_string"],  # example parameter
    # offset = int(os.environ["BATCHPAR_offset"])
    #
    # # reduce records in test mode
    # if test:
    #     limit = 50
    #     logging.info(f"Limiting to {limit} rows in test mode")
    # else:
    #     limit = batch_size
    #
    # logging.info(f"Processing {offset} - {offset + limit}")
    #
    # # database setup
    # logging.info(f"Using {db_name} database")

    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, batch_file)
    art_ids = json.loads(obj.get()['Body']._raw_stream.read())
    logging.info(f"{len(art_ids)} article IDs retrieved from s3")

    engine = get_mysql_engine("MYSQLDB", "mysqldb", db_name)
    # try_until_allowed(Base.metadata.create_all, engine)

    ids = ['00006CB7-61E0-4946-B7DB-DAE09ED63DE4', '0003C360-5DB8-4930-8266-6218C633A504',
           '000423DA-5A37-4E62-A698-703985CD4E6E', '000424B1-378E-4AB6-BAEC-CD8607AE08A9',
           '0004C698-0B51-4665-8593-1082BBF58C06', '0004CBDD-8A27-4FE1-B261-6018E79CFA49',
           '00053689-2879-4985-8E5C-4A3169D6CDE8', '00054883-FA93-46ED-AADF-E5F0C521035B',
           '000548C2-A1F1-47D7-8EA6-08DFEC94D4FF', '00065422-267E-44BB-A74C-84047BF0B3CD']

    with db_session(engine) as session:
        # consider moving this query and the one from the prepare step into a package
        batch_records = (session
                         .query(Projects.abstractText)
                         .filter(Projects.id.in_(ids))
                         .all())
    # Process and insert data
    processed_batch = docs2vectors([batch.abstractText for batch in batch_records])

    # logging.info(f"Inserting {len(processed_batch)} rows")
    # insert_data("BATCHPAR_config", 'mysqldb', db_name, Base, MyOtherTable,
    #             processed_batch, low_memory=True)
    #
    # logging.info(f"Marking task as done to {s3_path}")
    # s3 = boto3.resource('s3')
    # s3_obj = s3.Object(*parse_s3_path(s3_path))
    # s3_obj.put(Body="") # store vectors
    #
    # logging.info("Batch job complete.")


if __name__ == "__main__":
    # log_stream_handler = logging.StreamHandler()
    # logging.basicConfig(handlers=[log_stream_handler, ],
    #                     level=logging.INFO,
    #                     format="%(asctime)s:%(levelname)s:%(message)s")
    if 'BATCHPAR_done' not in os.environ:
        os.environ['BATCHPAR_db_name'] = 'dev'
    run()
