'''
Collect NIH
===========

Extract all of the NIH World RePORTER data via
their static data dump. :code:`N_TABS` outputs are produced
in CSV format (concatenated across all years), where
:code:`N_TABS` correspondes to the number of tabs in
the main table found at:

    https://exporter.nih.gov/ExPORTER_Catalog.aspx

The data is transferred to the Nesta intermediate data bucket.
'''

from bs4 import BeautifulSoup
import boto3
from io import BytesIO
from io import StringIO
import requests
from zipfile import ZipFile
import csv

# Some constants
BASE_URL = "https://exporter.nih.gov/"
TOP_URL = "https://exporter.nih.gov/ExPORTER_Catalog.aspx"
N_TABS = 5
S3 = boto3.resource('s3')


def get_data_urls(tab_index):
    '''Get all CSV URLs from the :code:`tab_index`th tab of
    the main table found at :code:`TOP_URL`.

    Args:
        tab_index (int): Tab number (0-indexed) of table to
             extract CSV URLs from.

    Returns:
        title (str): Title of the tab in the table.
        hrefs (list): List of URLs pointing to data CSVs.
    '''
    # Make the request process the response
    r = requests.get(TOP_URL, params={'index':tab_index})
    soup = BeautifulSoup(r.text, "html.parser")
    # Get the selected tab's title
    title = soup.find('a', class_='selected')['title']
    # Extract URLs if 'CSV' is in the URL, but ignore 'DUNS' data
    hrefs = []
    for a in soup.find_all('a', href=True):
        if ('CSVs' not in a['href']) or ('DUNS' in a['href']):
            continue
        if not a['href'].startswith('https'):
            a['href'] = '{}/{}'.format(BASE_URL, a['href'])
        hrefs.append(a['href'])
    return title, hrefs


def iterrows(url):
    '''Yield rows from the CSV (found at URL :code:`url`) as JSON (well, :code:`dict` objects).

    Args:
        url (str): The URL at which a zipped-up CSV is found.
    
    Yields:
        :code:`dict` object, representing one row of the CSV.
    '''

    # Get the CSV for this URL by unzipping the file at 'url'
    with BytesIO() as tmp_file:
        r = requests.get(url)
        tmp_file.write(r.content)
        with ZipFile(tmp_file) as tmp_zip:
            internal_file_names = tmp_zip.namelist()
            assert len(internal_file_names) == 1
            _data = tmp_zip.read(internal_file_names[0])

    # Yield JSON chunks of each CSV row
    #_data = _data.decode('cp1252').split("\r\n")
    _data = _data.decode('latin-1').split("\r\n")
    _data_it = csv.reader(_data)
    columns = next(_data_it)
    for row in _data_it:
        yield {col.lower(): val for col, val in zip(columns, row)}


if __name__ == '__main__':
    # Iterate over the number of tabs on the page
    for i in range(0, N_TABS+1):
        # Get the URLs which point to the data for this tab
        title, urls = get_data_urls(i)
        print(title)
        # Iterate over URLs
        for url in urls:
            print("\t", url)
            # Iterate over rows in the CSV
            for row in iterrows(urls):
                print("\t\t", row)
                break
            break
