import requests
from bs4 import BeautifulSoup
from datetime import datetime as dt
from gzip import GzipFile
from urllib.request import urlopen
import re

TOP_URL = "https://ftp.acc.umu.se/mirror/wikimedia.org/dumps/enwiktionary/{}"
FILENAME = "/enwiktionary-{}-all-titles-in-ns0.gz"
NON_ALPHA_PATTERN = re.compile(rb'[\W]+')
NON_BRACKET_PATTERN = re.compile(rb"[\(\[].*?[\)\]]")


def find_latest_wikidump():
    '''Identify the date (in the wikimedia dumps format)
    of the most recent wiktionary dump. Actually returns the secondmost
    recent date, as this is found to be more stable (e.g. if we make the
    request during the upload)

    Returns:
        wikidate (str): The most recent date (in the wikimedia dumps format)
    '''
    r = requests.get(TOP_URL.format(""))
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    max_date, max_date_str = None, None
    second_max_date_str = None
    for anchor in soup.find_all("a", href=True):
        raw_date = anchor.text.rstrip("/")
        try:
            date = dt.strptime(raw_date, '%Y%m%d')
        except ValueError:
            continue
        if max_date is None or date > max_date:
            second_max_date_str = max_date_str
            max_date = date
            max_date_str = raw_date
    if second_max_date_str is not None:
        return second_max_date_str
    return max_date_str


def extract_ngrams(date):
    '''Extract and reformat n-grams from wiktionary titles.
    Terms in parentheses are removed, and hyphens are converted
    to the standard n-gram separator (underscore). All other
    non-alphanumeric characters are then removed, and leading/trailing
    underscores are removed. Unigrams are then excluded.

    Args:
        date (str): A date string (in the wikimedia dumps format)

    Returns:
        ngrams (set): The set of n-grams from wiktionary
    '''
    r = urlopen((TOP_URL+FILENAME).format(date, date))
    ngrams = set()
    with GzipFile(fileobj=r) as gzio:
        for line in gzio:
            line = line.rstrip(b'\n')
            line = line.replace(b'-', b'_')
            line = NON_BRACKET_PATTERN.sub(b'', line)
            line = NON_ALPHA_PATTERN.sub(b'', line)
            if line.startswith(b'_'):
                line = line[1:]
            if line.endswith(b'_'):
                line = line[:-1]
            size = len(line.split(b'_'))
            if size == 1 or size > 6:
                continue
            if len(line) > 50:
                continue
            if line.decode('utf-8')[0].isnumeric():
                continue
            ngrams.add(line.lower())
    return ngrams


if __name__ == "__main__":
    wiki_date = find_latest_wikidump()        
    ngrams = extract_ngrams(wiki_date)
    print(f"Found {len(ngrams)} n-grams")
