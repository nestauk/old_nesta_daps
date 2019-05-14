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

from nesta.packages.mag.query_mag_api import prepare_title
from nesta.packages.misc_utils.batches import split_batches
from nesta.production.orms.orm_utils import get_mysql_engine, try_until_allowed
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
    r.raise_for_status()
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
    new_token = root.find(OAI+'ListRecords').find(OAI+"resumptionToken")
    if new_token is None or new_token.text is None:
        resumption_token = None
        logging.info(f"Hit end of arXiv data. {len(output)} records in this final batch.")
    else:
        if resumption_token is None:  # first batch only
            total_articles = new_token.attrib.get('completeListSize', 0)
            logging.info(f"Total records to retrieve in batches: {total_articles}")
        resumption_token = new_token.text
        logging.info(f"next resumptionCursor: {resumption_token.split('|')[1]}")

    return output, resumption_token


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


def extract_last_update_date(prefix, updates):
    """Determine the latest valid date from a list of update_ids.
    Args:
        prefix (str): valid prefix in the update id
        updates (list of str): update ids extracted from the luigi_table_updates

    Returns:
        (str): latest valid date from the supplied list
    """
    date_pattern = r'(\d{4}-\d{2}-\d{2})'
    pattern = re.compile(f'^{prefix}_{date_pattern}$')
    matches = [re.search(pattern, update) for update in updates]
    dates = []
    for match in matches:
        try:
            dates.append(datetime.datetime.strptime(match.group(1), '%Y-%m-%d'))
        except (AttributeError, ValueError):  # no matches or strptime conversion fail
            pass
    try:
        return sorted(dates)[-1]
    except IndexError:
        raise ValueError("Latest date could not be identified")


class BatchedTitles():
    def __init__(self, ids, batch_size, session):
        """Extracts batches of titles from the database and yields titles for lookup
        against the MAG api.

        A lookup dict of {prepared title: [id, ...]} is generated for each batch:
        self.title_id_lookup

        Args:
            ids (set): all article ids to be processed
            batch_size (int): number of ids in each query to the database
            session (:obj:`sqlalchemy.orm.session`): current session

        Returns:
            (generator): yields single prepared titles
        """
        self.ids = ids
        self.batch_size = batch_size
        self.session = session
        self.title_articles_lookup = defaultdict(list)

    def __getitem__(self, key):
        """Get articles which match the provided title.
        Args:
            key (str): the title of the article

        Returns
            (:obj:`list` of `str`) article ids which matched the provided title before
            the request to mag was made.
        """
        matching_articles = self.title_articles_lookup.get(key)
        if matching_articles is None:
            raise KeyError(f"Title not found in lookup {key}")
        else:
            return matching_articles

    def __iter__(self):
        for batch_of_ids in split_batches(self.ids, self.batch_size):
            self.title_articles_lookup.clear()
            for article in (self.session.query(Article)
                                        .filter(Article.id.in_(batch_of_ids))
                                        .all()):
                self.title_articles_lookup[prepare_title(article.title)].append(article.id)

            for title in self.title_articles_lookup:
                yield title


def add_new_articles(article_batch, session):
    """Adds new articles to the session and commits them.
    Args:
        article_batch (:obj:`list` of `Article`): Articles to add to database
        session (:obj:`sqlalchemy.orm.session`): active session to use
    """
    logging.info(f"Inserting a batch of {len(article_batch)} new Articles")
    session.add_all(article_batch)
    session.commit()


def update_existing_articles(article_batch, session):
    """Updates existing articles from a list of dictionaries. Bulk method is used for
    non relationship fields, with the relationship fields updated using the core orm
    method.

    Args:
        article_batch (:obj:`list` of `dict`): articles to add to database
        session (:obj:`sqlalchemy.orm.session`): active session to use
    """
    logging.info(f"Updating a batch of {len(article_batch)} existing articles")

    # convert lists of category ids into rows for association table
    article_categories = [dict(article_id=article['id'], category_id=cat_id)
                          for article in article_batch
                          for cat_id in article.pop('categories', [])]

    # convert lists of fos_ids into rows for association table
    article_fields_of_study = [dict(article_id=article['id'], fos_id=fos_id)
                               for article in article_batch
                               for fos_id in article.pop('fields_of_study', [])]

    # convert lists of institutes into rows for association table
    article_institutes = [dict(article_id=article['id'], institude_id=institute_id)
                          for article in article_batch
                          for institute_id in article.pop('institutes', [])]

    # update unlinked article data in bulk
    logging.debug("bulk update mapping on articles")
    session.bulk_update_mappings(Article, article_batch)

    logging.debug("core orm delete and insert on categories")
    if article_categories:
        # remove and re-create links
        article_cats_table = Base.metadata.tables['arxiv_article_categories']
        all_article_ids = {a['id'] for a in article_batch}
        logging.debug("core orm delete on categories")
        session.execute(article_cats_table.delete()
                        .where(article_cats_table.columns['article_id'].in_(all_article_ids)))
        logging.debug("core orm insert on categories")
        session.execute(article_cats_table.insert(),
                        article_categories)

    logging.debug("core orm insert on fields of study")
    if article_fields_of_study:
        session.execute(Base.metadata.tables['arxiv_article_fields_of_study'].insert(),
                        article_fields_of_study)

    logging.debug("core orm insert on institutes")
    if article_institutes:
        session.execute(Base.metadata.tables['arxiv_article_institutes'].insert(),
                        article_institutes)

    session.commit()


if __name__ == '__main__':
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")
