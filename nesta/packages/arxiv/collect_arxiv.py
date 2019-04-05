from collections import defaultdict
import datetime
import json
import logging
import pandas as pd
import re
import requests
from retrying import retry
import s3fs  # required for pandas to use read_csv from s3
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
import time
import xml.etree.ElementTree as ET

from nesta.packages.mag.query_mag import prepare_title
from nesta.packages.misc_utils.batches import split_batches
from nesta.production.orms.orm_utils import get_mysql_engine, try_until_allowed, db_session
from nesta.production.orms.arxiv_orm import Base, Article, Category

OAI = "{http://www.openarchives.org/OAI/2.0/}"
ARXIV = "{http://arxiv.org/OAI/arXiv/}"
DELAY = 10  # seconds between requests
API_URL = 'http://export.arxiv.org/oai2'


def _category_exists(session, cat_id):
    """Checks if an arxiv category exists in the categories table.
    Args:
        session (:obj:`sqlalchemy.orm.session`): sqlalchemy session
        cat_id (str): id of the arxiv category

    Returns:
        (bool): True if the id is already in the database, otherwise False
    """
    try:
        session.query(Category).filter(Category.id == cat_id).one()
    except NoResultFound:
        return False
    return True


def _add_category(session, cat_id, description):
    """Adds new categories to the database and commits them.

    Args:
        session (:obj:`sqlalchemy.orm.session`): sqlalchemy session
        cat_id (str): id of the arxiv category
        description (str): description of the category
    """
    logging.info(f"adding {cat_id} to database")
    session.add(Category(id=cat_id, description=description))
    session.commit()


def load_arxiv_categories(db_config, db, bucket, cat_file):
    """Loads a file of categories and descriptions into mysql from a csv file on s3.

    Args:
        db_config (str): environmental variable pointing to mysql config file
        db (str): config header to use from the mysql config file
        bucket (str): s3 bucket where the csv is held
        cat_file (str): path to the file on s3
    """
    target = f's3://{bucket}/{cat_file}'
    categories = pd.read_csv(target)

    # Setup the database connectors
    engine = get_mysql_engine(db_config, "mysqldb", db)
    try_until_allowed(Base.metadata.create_all, engine)
    Session = try_until_allowed(sessionmaker, engine)
    session = try_until_allowed(Session)

    logging.info(f'found {session.query(Category).count()} existing categories')
    for idx, data in categories.iterrows():
        if not _category_exists(session, data['id']):
            _add_category(session, cat_id=data['id'], description=data['description'])
    session.close()


@retry(stop_max_attempt_number=10)
def _arxiv_request(url, delay=DELAY, **kwargs):
    """Make a request and convert the result to xml elements.

    Args:
        url (str): endpoint of the api
        delay (int): time to wait after request in seconds
        kwargs (dict): any other arguments to pass as paramaters in the request

    Returns:
        (:obj:`xml.etree.ElementTree.Element`): converted response
    """
    params = dict(verb='ListRecords', **kwargs)
    r = requests.get(url, params=params)
    time.sleep(delay)
    try:
        root = ET.fromstring(r.text)
    except ET.ParseError as e:
        logging.error(r.text)
        raise e
    return root


def total_articles():
    """Make a request to the API and capture the total number of articles from the
    resumptionToken tag.

    Returns:
        (int): total number of articles
    """
    root = _arxiv_request(API_URL, metadataPrefix='arXiv')
    token = root.find(OAI+'ListRecords').find(OAI+"resumptionToken")
    list_size = token.attrib['completeListSize']
    return int(list_size)


def request_token():
    """Make a request to the API and capture the resumptionToken which will be used to
    page through results. Not efficient as it also returns the first page of data which
    is slow and not required.

    Returns:
        (str): supplied resumtionToken
    """
    root = _arxiv_request(API_URL, metadataPrefix='arXiv')
    token = root.find(OAI+'ListRecords').find(OAI+"resumptionToken")
    resumption_token = token.text.split("|")[0]
    logging.info(f"resumptionToken: {resumption_token}")
    return resumption_token


def xml_to_json(element, tag, prefix=''):
    """Converts a layer of xml to a json string. Handles multiple instances of the
    specified tag and any schema prefix which they may have.

    Args:
        element (:obj:`xml.etree.ElementTree.Element`): xml element containing the data
        tag (str): target tag
        prefix (str): schema prefix on the name of the tag

    Returns:
        (str): json of the original object with the supplied tag as the key and a list
               of all instances of this tag as the value.
    """
    tag = ''.join([prefix, tag])

    all_data = [{field.tag[len(prefix):]: field.text
                for field in fields.getiterator()
                if field.tag != tag}
                for fields in element.getiterator(tag)]
    return json.dumps(all_data)


def arxiv_batch(resumption_token=None, **kwargs):
    """Retrieves a batch of data from the arXiv api (expect up to 1000).
    If a resumption token and cursor are not supplied then the first batch will be
    requested. Additional keyword arguments are only sent with the first request (ie no
    resumption_token)

    Args:
        resumption_token (str): resumptionToken issued from a prevous request
        kwargs: additonal paramaters to send with the initial api request

    Returns:
        (:obj:`list` of :obj:`dict`): retrieved records
        (str or None): resumption token if present in results
    """
    if resumption_token is not None:
        root = _arxiv_request(API_URL, resumptionToken=resumption_token)
    else:
        root = _arxiv_request(API_URL, metadataPrefix='arXiv', **kwargs)
    records = root.find(OAI+'ListRecords')
    output = []

    for record in records.findall(OAI+"record"):
        header = record.find(OAI+'header')
        header_id = header.find(OAI+'identifier').text
        row = dict(datestamp=header.find(OAI+'datestamp').text)
        logging.debug(f"article {header_id} datestamp: {row['datestamp']}")

        # collect all fields from the metadata section
        meta = record.find(OAI+'metadata')
        if meta is None:
            logging.warning(f"No metadata for article {header_id}")
        else:
            info = meta.find(ARXIV+'arXiv')
            fields = ['id', 'created', 'updated', 'title', 'categories',
                      'journal-ref', 'doi', 'msc-class', 'abstract']
            for field in fields:
                try:
                    row[field.replace('-', '_')] = info.find(ARXIV+field).text
                except AttributeError:
                    logging.debug(f"{field} not found in article {header_id}")

            for date_field in ['datestamp', 'created', 'updated']:
                try:
                    date = row[date_field]
                    row[date_field] = datetime.datetime.strptime(date, '%Y-%m-%d').date()
                except ValueError:
                    del row[date_field]  # date is not in the correct format
                except KeyError:
                    pass  # field not in this row

            # split categories into a list for the link table in sql
            try:
                row['categories'] = row['categories'].split(' ')
                row['title'] = row['title'].strip()
                row['authors'] = xml_to_json(info, 'author', prefix=ARXIV)
            except KeyError:
                pass  # field not in this row

        output.append(row)

    # extract cursor for next batch
    token = root.find(OAI+'ListRecords').find(OAI+"resumptionToken")
    if resumption_token is None:  # first batch only
        logging.info(f"Total records to retrieve: {token.attrib['completeListSize']}")
    if token.text is not None:
        logging.info(f"next resumptionCursor: {token.text.split('|')[1]}")
    else:
        logging.info("End of data")

    return output, token.text


def retrieve_arxiv_batch_rows(start_cursor, end_cursor, token):
    """Iterate through batches and yield single rows until the end_cursor or end of data
    is reached.

    Args:
        start_cursor (int): first record to return
        end_cursor (int): start of the next batch, ie stop when this cursor is returned
        resumption_token (int): token to supply the api

    Returns:
        (dict): a single row of data
    """
    resumption_token = '|'.join([token, str(start_cursor)])
    while resumption_token is not None and start_cursor < end_cursor:
        batch, resumption_token = arxiv_batch(resumption_token)
        if resumption_token is not None:
            start_cursor = int(resumption_token.split("|")[1])
        for row in batch:
            yield row


def retrieve_all_arxiv_rows(**kwargs):
    """Iterate through batches and yield single rows through the whole dataset.

    Args:
        kwargs: any keyword arguments to send with the request

    Returns:
        (dict): a single row of data
    """
    resumption_token = None
    while True:
        batch, resumption_token = arxiv_batch(resumption_token, **kwargs)
        for row in batch:
            yield row
        if resumption_token is None:
            break


def batched_titles(ids, title_id_lookup, batch_size, engine):
    """Extracts batches of titles from the database and yields titles for lookup against
    the MAG api. A lookup dict of titles to ids is also appended to a supplied
    defaultdict (duplicate titles exist in the arXiv dataset.)

    Args:
        ids (set): all article ids to be processed
        title_id_lookup (:obj:`collections.defaultdict`): an empty defaultdict(list)
            where generated {title:[id, ...]} lookup will be appended
        batch_size (int): number of ids in each query to the database
        engine (:obj:`sqlalchemy.engine.base.Engine`): database connection engine

    Returns:
        (generator): yields single prepared titles
    """
    for batch_of_ids in split_batches(ids, batch_size):
        with db_session(engine) as session:
            batch_with_titles = (session
                                 .query(Article.id, Article.title)
                                 .filter(Article.id.in_(batch_of_ids))
                                 .all())
        clean_titles = set()
        for (id, title) in batch_with_titles:
            title = prepare_title(title)
            clean_titles.add(title)
            title_id_lookup[title].append(id)

        for title in clean_titles:
            yield title


if __name__ == '__main__':
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")
