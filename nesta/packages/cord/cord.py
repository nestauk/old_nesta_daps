from tempfile import TemporaryFile
import os.path
import requests
import shutil
import tarfile
import csv
import re
from io import StringIO

CORD_URL = ('https://ai2-semanticscholar-cord-19.'
            's3-us-west-2.amazonaws.com/historical_releases/'
            'cord-19_{date}.tar.gz')
CSV_RE = '{date}/(.*)metadata(.*).csv'  # Name changes over time, so reqs a regex
AWS_TMP_DIR = '/dev/shm/'

def stream_to_file(url, fileobj):
    """
    Stream a large file from a URL to a file object in a memory-efficient
    rate-efficient manner.
    """
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        shutil.copyfileobj(r.raw, fileobj)
    fileobj.seek(0)  # Reset the file pointer, ready for reading


def cord_csv(date):
    """Load the CORD19 metadata CSV file for the given date string"""
    # Prepare variables
    url = CORD_URL.format(date=date)
    csv_re = re.compile(CSV_RE.format(date=date))
    tmp_dir = AWS_TMP_DIR if os.path.isdir(AWS_TMP_DIR) else None
    # Stream the huge tarball into the local tempfile
    with TemporaryFile(dir=tmp_dir, suffix='.tar.gz') as fileobj:
        stream_to_file(url, fileobj)
        # Filter out the CSV file based on regex
        tf = tarfile.open(fileobj=fileobj)
        match, = filter(None, map(csv_re.match, tf.getnames()))
        # Load the CSV file data into memory
        csv = tf.extractfile(match.group())
        return StringIO(csv.read().decode('latin'))  # Return wipes the cache

def cord_data(date):
    """
    Yields lines from the CORD19 metadata CSV file 
    for the given date string
    """
    with cord_csv(date) as f:
        for line in csv.DictReader(f):
            yield line
