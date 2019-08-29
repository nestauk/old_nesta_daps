import os
import json
import logging

import boto3
from ast import literal_eval
from urllib.parse import urlsplit

from nesta.core.orms.gtr_orm import Projects
from nesta.core.orms.orm_utils import db_session
from nesta.packages.nlp_utils.text2vec import docs2vectors
from nesta.core.orms.orm_utils import get_mysql_engine


def parse_s3_path(path):
    '''For a given S3 path, return the bucket and key values'''
    parsed_path = urlsplit(path)
    s3_bucket = parsed_path.netloc
    s3_key = parsed_path.path.lstrip('/')
    return (s3_bucket, s3_key)


def run():
    test = literal_eval(os.environ["BATCHPAR_test"])
    db_name = os.environ["BATCHPAR_db_name"]
    bucket = os.environ['BATCHPAR_bucket']
    batch_file = os.environ['BATCHPAR_batch_file']
    outinfo = os.environ["BATCHPAR_outinfo"]
    output_bucket = 'clio-text2vec'

    # reduce records in test mode
    if test:
        limit = 50
        logging.info(f"Limiting to {limit} rows in test mode")
    else:
        limit = None
    # database setup
    logging.info(f"Using {db_name} database")

    # Get IDs from S3
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, batch_file)
    ids = json.loads(obj.get()['Body']._raw_stream.read())
    logging.info(f"{len(ids)} article IDs retrieved from s3")

    # Connect to SQL
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db_name)
    with db_session(engine) as session:
        batch_records = (session
                         .query(Projects.abstractText)
                         .filter(Projects.id.in_(ids))
                         # .limit(limit)
                         .all())

    # Process and insert data
    vectors = docs2vectors([batch.abstractText for batch in batch_records])
    processed_batch = {id_: vector.tolist() for id_, vector in zip(ids, vectors)}
    logging.info(f"Inserting {len(processed_batch)} rows")

    # Store batched vectors in S3
    s3 = boto3.resource('s3')
    obj = s3.Object(output_bucket, f'{outinfo}.json')
    obj.put(Body=json.dumps(processed_batch))
