from ast import literal_eval
import boto3
import logging
import json
import os
from urllib.parse import urlsplit

from nesta.production.orms.gtr_orm import Projects
from nesta.production.orms.orm_utils import get_mysql_engine
from nesta.production.orms.orm_utils import db_session
from nesta.packages.nlp_utils.text2vec import docs2vectors


def parse_s3_path(path):
    '''For a given S3 path, return the bucket and key values'''
    parsed_path = urlsplit(path)
    s3_bucket = parsed_path.netloc
    s3_key = parsed_path.path.lstrip('/')
    return (s3_bucket, s3_key)


def run():
    test = literal_eval(os.environ["BATCHPAR_test"])
    db_name = os.environ["BATCHPAR_db_name"]
    # db_name = 'dev'
    # db_config = os.environ["BATCHPAR_config"]
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
    # database setup - this is my print
    logging.info(f"Using {db_name} database")

    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, batch_file)
    ids = json.loads(obj.get()['Body']._raw_stream.read())
    logging.info(f"{len(ids)} article IDs retrieved from s3")

    engine = get_mysql_engine("MYSQLDB", "mysqldb", db_name)
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
    s3 = boto3.resource('s3')
    obj = s3.Object(output_bucket, f'{outinfo}.json')
    obj.put(Body=json.dumps(processed_batch))


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")

    # set environ to run it locally
    if 'BATCHPAR_outinfo' not in os.environ:
        environ = {'batch_file': ('test-ids.json'),
                   # 'config': 'mysqldb.config',
                   'db_name': 'dev',
                   'bucket': 'nesta-production-intermediate',
                   'done': "False",
                   'outinfo': ('test-text2vectors'),
                   'aws_auth_region': 'eu-west-2',
                   'test': "True"}
        for k, v in environ.items():
            os.environ[f'BATCHPAR_{k}'] = v

    run()
