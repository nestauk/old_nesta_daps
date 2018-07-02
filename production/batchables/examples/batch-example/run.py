import boto3
from urllib.parse import urlsplit
import json
import os


def parse_s3_path(path):
    "For a given S3 path, return the bucket and key values"
    parsed_path = urlsplit(path)
    s3_bucket = parsed_path.netloc
    s3_key = parsed_path.path.lstrip('/')
    print("-->", s3_bucket, s3_key)
    return (s3_bucket, s3_key)


def run(self):
    # Get parameters for the batch job
    outpath = os.environ("BATCHPAR_outinfo")
    age = os.environ("BATCHPAR_age")
    name = os.environ("BATCHPAR_name")
    print(outpath, age, name, sep="\n")
    # Generate the output json
    data = json.dumps({"name":name, "age":age+1}).encode('utf8')
    print(data)
    # Upload the data to S3
    s3 = boto3.resource('s3')    
    s3_obj = s3.Object(*parse_s3_path(outpath))
    s3_obj.put(Body=data)
