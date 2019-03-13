from nesta.production.luigihacks.s3 import parse_s3_path

import os
import boto3
import json
import numpy as np
import json


def run():
    # Get variables out
    s3_path_in = os.environ['BATCHPAR_s3_path_in']
    s3_path_out = os.environ["BATCHPAR_outinfo"]
    first_index = int(os.environ['BATCHPAR_first_index'])
    last_index = int(os.environ['BATCHPAR_last_index'])

    # Load the data
    s3 = boto3.resource('s3')
    #s3_obj_in = s3.Object(*parse_s3_path(s3_path_in))
    #data = json.load(s3_obj_in.get()['Body'])

    # Mark the task as done
    if s3_path_out != "":
        s3 = boto3.resource('s3')
        s3_obj = s3.Object(*parse_s3_path(s3_path_out))
        s3_obj.put(Body=json.dumps(["DUMMY", "JSON"]))


if __name__ == "__main__":
    run()
