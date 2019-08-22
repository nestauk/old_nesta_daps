'''
run.py (batch_example)
======================

The batchable for the :code:`routines.examples.batch_example`,
which simply increments a muppet's age by one unit.
'''

import boto3
from urllib.parse import urlsplit
import json
import os
from ast import literal_eval
from world_reporter_api import get_abstract
import time
from requests.exceptions import SSLError
from random import randint
import logging

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def parse_s3_path(path):
    '''For a given S3 path, return the bucket and key values'''
    parsed_path = urlsplit(path)
    s3_bucket = parsed_path.netloc
    s3_key = parsed_path.path.lstrip('/')
    return (s3_bucket, s3_key)


def run():
    '''Gets the name and age of the muppet, and increments the age.
    The result is transferred to S3.'''

    # Get parameters for the batch job
    outpath = os.environ["BATCHPAR_outinfo"]
    program_ids = literal_eval(os.environ["BATCHPAR_data"])

    # 
    outdata = []
    for pid in program_ids:
        logging.info("--> %s" % pid)
        n_error = 0
        while True:
            try:
                abstract = get_abstract(pid)
            except SSLError:
                n_error += 1
                assert n_error < 10
                time.sleep(randint(1,10))
                logging.warning("--> retrying %s" % pid)
                continue
            else:
                break
        row = dict(abstract_text=abstract,
                   program_id=pid)
        time.sleep(randint(1,10))
        outdata.append(row)
    data = json.dumps(outdata).encode('utf8')

    # Upload the data to S3
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(*parse_s3_path(outpath))
    s3_obj.put(Body=data)


if __name__ == "__main__":
    run()

