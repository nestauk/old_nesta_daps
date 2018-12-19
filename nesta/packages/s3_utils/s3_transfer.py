import boto3
import math

from nesta.packages.misc_tools.misc import chunker


def get_latest(bucket, key=None):
    ''' get_latest
    Retrieves the most recent file object from a bucket.

    Args:
        bucket (str): S3 bucket name
        key (str): key to filter objects in bucket

    Returns
        latest (:obj:`boto3.resources.factory.s3.ObjectSummary`): object for 
            most recent file
    '''

    s3 = boto3.resource('s3')
    if key is None:
        objs = s3.Bucket(bucket).objects.all()
    else:
        objs = s3.Bucket(bucket).objects.filter(Prefix=key)

    get_last_modified = lambda obj: int(obj.last_modified.strftime('%s'))
    latest = [obj for obj in sorted(objs, key=get_last_modified)][0]
    return latest

def chunk_iterable_to_txt(iterable, bucket, key_prefix, n=10000):
    ''' chunk_iterable_to_txt
    Groups the elements of an iterable into n-sized chunks and writes them to
    newline delimited text files in S3.

    Args:
        iterable (iter): an iterable
        bucket (str): s3 bucket name
        key_prefix (str): key prefix for the files. Can include braces which
            will be formatted with the chunk number.

    '''
    s3_client = boto3.client('s3')

    chunks = chunker(iterable, n)

    for i, chunk in enumerate(chunks):
        body = '\n'.join(chunk).strip()
        if '{}' in key_prefix:
            key = key_prefix.format(i)
        else:
            key = key_prefix + f'_{i}'
        response = s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=body,
                )
        yield key

