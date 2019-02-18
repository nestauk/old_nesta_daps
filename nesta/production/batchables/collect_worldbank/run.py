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
from nesta.packages.worldbank.collect_worldbank import country_data_single_request
import ast

def parse_s3_path(path):
    '''For a given S3 path, return the bucket and key values'''
    parsed_path = urlsplit(path)
    s3_bucket = parsed_path.netloc
    s3_key = parsed_path.path.lstrip('/')
    return (s3_bucket, s3_key)


def run():
    '''Gets the name and age of the muppet, and increments the age.
    The result is transferred to S3.'''

    # Remove the 2 default parameters for the batch job
    outpath = os.environ.pop("BATCHPAR_outinfo")
    _ = os.environ.pop("BATCHPAR_done")

    kwargs = {}
    for k, v in os.environ.items():
        if not k.startswith("BATCHPAR_"):
            continue
        if k.isupper():
            continue
        new_key = k.replace("BATCHPAR_", "")
        if v.isdigit():
            v = int(v)
        kwargs[new_key] = v
    kwargs['data_key_path'] = ast.literal_eval(kwargs['data_key_path'])
    print("===>", kwargs)

    country_data = country_data_single_request(**kwargs)

    # Generate the output json
    data = json.dumps(country_data).encode('utf8')
    # Upload the data to S3
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(*parse_s3_path(outpath))
    s3_obj.put(Body=data)


if __name__ == "__main__":
    run()
