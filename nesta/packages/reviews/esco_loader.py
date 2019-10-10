import csv
import requests
import logging
from nesta.packages.reviews.esco_links_config import esco_links_dict as links

def load_csv_as_dict(url):
    """
    Loads CSV data from web as a list of dictionaries
    Args:
        url (str): path to the CSV file
    """
    download = requests.Session().get(url)
    cr = csv.DictReader(download.content.decode('utf-8').splitlines(),
                        delimiter=',')
    return list(cr)

def add_new_rows(row_batch, session):
    """
    Adds new data rows to the session and commits them.
    Args:
        row_batch (:obj:`list` of `Article`): rows to add to database
        session (:obj:`sqlalchemy.orm.session`): active session to use
    """
    logging.info(f"Inserting a batch of size {len(row_batch)}")
    session.add_all(row_batch)
    session.commit()
