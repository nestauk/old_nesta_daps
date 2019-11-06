'''
run.py (collect_worldbank)
==========================

The batchable for the collecting worldbank data.
'''

import boto3
from urllib.parse import urlsplit
import json
import os
from nesta.packages.worldbank.collect_worldbank import country_data_single_request
import ast
from nesta.core.luigihacks.s3 import parse_s3_path

def run():
    '''Make a single request for country's worldbank data'''

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
