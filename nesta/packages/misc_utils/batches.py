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


class BatchWriter(list):
    """A list with functionality to monitor appends.
    When a specified size is reached a function is called and the list cleared down.
    """
    def __init__(self, limit, function, *function_args, **function_kwargs):
        """
        Args:
            limit (int): limit in length triggering a write
            function (function): the function which will be called
            args, kwargs: will be passed on to the function call
        """
        self.limit = limit
        self.function = function
        self.function_args = function_args
        self.function_kwargs = function_kwargs

    def append(self, item):
        """Appends to the list and then checks current size against the set limit."""
        super().append(item)
        if len(self) >= self.limit:
            self.write()

    def extend(self, items):
        """Adds multiple items to the list then writes until it is below limit."""
        super().extend(items)
        while len(self) >= self.limit:
            self.write(self.limit)

    def write(self, n=None):
        """Calls the specified function with args and kwargs"""
        self.function(self[0:n], *self.function_args, **self.function_kwargs)
        del self[0:n]
