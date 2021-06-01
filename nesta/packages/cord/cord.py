from tempfile import TemporaryFile
import os.path
import requests
import shutil
import tarfile
import csv
import re
from io import StringIO


LATEST_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
CSV_RE = "{date}/(.*)metadata(.*).csv"  # Name changes over time
BASE_URL = (
    "https://ai2-semanticscholar-cord-19."
    "s3-us-west-2.amazonaws.com/historical_releases{}"
)
HTML_URL = BASE_URL.format(".html")
DATA_URL = BASE_URL.format("/cord-19_{date}.tar.gz")
AWS_TMP_DIR = "/dev/shm/"
CORD_TO_ARXIV_LOOKUP = {
    "datestamp": "publish_time",
    "created": "publish_time",
    "updated": "publish_time",
    "title": "title",
    "journal_ref": "journal",
    "doi": "doi",
    "abstract": "abstract",
}


def stream_to_file(url, fileobj):
    """
    Stream a large file from a URL to a file object in a memory-efficient
    rate-efficient manner.
    """
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        shutil.copyfileobj(r.raw, fileobj)
    fileobj.seek(0)  # Reset the file pointer, ready for reading


def cord_csv(date=None):
    """Load the CORD19 metadata CSV file for the given date string"""
    # Prepare variables
    if date is None:
        date = most_recent_date()

    url = DATA_URL.format(date=date)
    csv_re = re.compile(CSV_RE.format(date=date))
    tmp_dir = AWS_TMP_DIR if os.path.isdir(AWS_TMP_DIR) else None
    # Stream the huge tarball into the local tempfile
    with TemporaryFile(dir=tmp_dir, suffix=".tar.gz") as fileobj:
        stream_to_file(url, fileobj)
        # Filter out the CSV file based on regex
        tf = tarfile.open(fileobj=fileobj)
        (match,) = filter(None, map(csv_re.match, tf.getnames()))
        # Load the CSV file data into memory
        csv = tf.extractfile(match.group())
        return StringIO(csv.read().decode("utf-8"))  # Return wipes the cache


def cord_data(date=None):
    """
    Yields lines from the CORD19 metadata CSV file
    for the given date string.

    Note: takes 20 mins over grounded internet for a ~10GB tarball
    """
    with cord_csv(date) as f:
        for line in csv.DictReader(f):
            yield line


def most_recent_date():
    response = requests.get(HTML_URL)
    response.raise_for_status()
    return LATEST_RE.search(response.text).group()


def to_arxiv_format(cord_row):
    arxiv_row = {
        arxiv_key: cord_row[cord_key]
        for arxiv_key, cord_key in CORD_TO_ARXIV_LOOKUP.items()
    }
    authors = cord_row["authors"].split(";")
    arxiv_row["id"] = f"cord-{cord_row['cord_uid']}"
    arxiv_row["authors"] = list(map(str.strip, authors))
    arxiv_row["article_source"] = "cord"  # hard-coded
    return arxiv_row
