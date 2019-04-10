"""Utilties for working with batches."""
import boto3
import json
import time


def split_batches(data, batch_size):
    """Breaks batches down into chunks consumable by the database.

    Args:
        data (:obj:`iterable`) iterable containing data items
        batch_size (int): number of items per batch

    Returns:
        (:obj:`list` of :obj:`dict`): yields a batch at a time
    """
    batch = []
    for row in data:
        batch.append(row)
        if len(batch) == batch_size:
            yield batch
            batch.clear()
    if len(batch) > 0:
        yield batch

def put_s3_batch(data, bucket, prefix):
        """Writes out a batch of data to s3 as json, so it can be picked up by the
        batchable task.
        Args:
            data (:obj:`list` of :obj:`dict`): a batch of records
        Returns:
            (str): name of the file in the s3 bucket (key)
        """
        # s3 setup
        s3 = boto3.resource('s3')

        timestamp = str(time.time()).replace('.', '')
        filename = f"{prefix}-{timestamp}.json"
        obj = s3.Object(bucket, filename)
        obj.put(Body=json.dumps(data))
        return filename