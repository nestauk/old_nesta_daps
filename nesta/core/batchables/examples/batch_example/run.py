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
from nesta.core.luigihacks.s3 import parse_s3_path


def run():
    '''Gets the name and age of the muppet, and increments the age.
    The result is transferred to S3.'''

    # Get parameters for the batch job
    outpath = os.environ["BATCHPAR_outinfo"]
    age = int(os.environ["BATCHPAR_age"])
    name = os.environ["BATCHPAR_name"]
    # Generate the output json
    data = json.dumps({"name": name, "age": age+1}).encode('utf8')
    # Upload the data to S3
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(*parse_s3_path(outpath))
    s3_obj.put(Body=data)


if __name__ == "__main__":
    run()
