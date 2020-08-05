"""
run.py (nlp.bert_vectorize)
===========================

Vectorize text documents via BERT
"""

from ast import literal_eval
import boto3
import json
import logging
import os

from nesta.core.orms.orm_utils import db_session, get_mysql_engine
from nesta.core.orms.orm_utils import insert_data
from nesta.core.orms.orm_utils import get_class_by_tablename
from nesta.core.orms.orm_utils import get_base_by_tablename


def run():
    test = literal_eval(os.environ["BATCHPAR_test"])
    bucket = os.environ['BATCHPAR_bucket']
    batch_file = os.environ['BATCHPAR_batch_file']
    db_name = os.environ["BATCHPAR_db_name"]
    os.environ["MYSQLDB"] = os.environ["BATCHPAR_config"]

    # Database setup
    engine = get_mysql_engine("MYSQLDB", "mysqldb", db_name)

    # Retrieve list of Org ids from S3
    nrows = 20 if test else None
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, batch_file)
    _ids = json.loads(obj.get()['Body']._raw_stream.read())
    logging.info(f"{len(_ids)} objects retrieved from s3")

    in_class = get_class_by_tablename(in_tablename)
    in_Base = get_base_by_tablename(in_tablename)
    out_class = get_class_by_tablename(out_tablename)
    out_Base = get_base_by_tablename(out_tablename)

    # create_all
    # retrieve all
    # process all
    # insert_data
    

if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")
    run()
