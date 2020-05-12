'''
Collection task
===============

Luigi routine to collect new data from the arXiv api and load it to MySQL.
'''
from datetime import datetime as dt
from datetime import timedelta
import luigi
import logging
import pandas as pd

from nesta.packages.magrxiv.collect_magrxiv import get_magrxiv_articles
from nesta.core.orms.arxiv_orm import Article, Base
from nesta.core.orms.orm_utils import get_mysql_engine, db_session, insert_data
from nesta.core.luigihacks import misctools
from nesta.core.luigihacks.mysqldb import MySqlTarget

def get_article_ids(session, xivs):
    """Get the set of all article IDs in the current database.

    Args:
        session (SqlAlchemy connectable): SqlAlchemy db session for querying
        xivs (list): List of valid 'article_source' values to include in the filtering.
    Returns:
        ids (set): set of all article IDs in the current database for these article_sources.
    """
    xiv_filter = Article.article_source.in_(xivs)
    return {article.id for article in
            session.query(Article.id).filter(xiv_filter).all()}


def get_articles(xivs, api_key, start_date, article_ids, test, flush_every=1000):
    """Get all articles from MAG from specified 'Journals' (xivs).

    Args:
        xivs (list): List of valid MAG 'Journals' to query.
        api_key (str): MAG API key
        start_date (str): Sensibly formatted date string (interpretted by pd)
        article_ids (set): Set of article IDs to skip.
        test (bool): Running in test mode?
        flush_every (int): Sets the number of articles retrieved between log messages,
                           and also related to the maximum number of articles acquired
                           in test mode (NB: in test mode at most 2 x flush_every will be collected).
    Returns:
        articles (:obj:`list` of :obj:`dict`): List of articles from MAG, formatted a-la-arxiv.
    """
    logging.info(f"{len(article_ids)} existing articles will not be recollected")
    article_ids = article_ids.copy()
    articles = []
    for xiv in xivs:
        _articles = _get_articles(xiv, api_key, start_date, article_ids, test, flush_every)
        article_ids.update([row['id'] for row in _articles])
        articles += _articles
        # Note ">" rather than ">=" so that more than one xiv can be guaranteed to be in the test
        if test and len(articles) > flush_every:
            logging.info(f"Limiting to {len(articles)} rows while in test mode")
            break
        logging.info(f"Retrieved {len(articles)} articles so far")
    return articles


def _get_articles(xiv, api_key, start_date, article_ids, test, flush_every):
    """Get all articles from MAG from one specified 'Journal' (xiv).

    Args:
        xiv (str): A valid MAG 'Journal' to query.
        api_key (str): MAG API key
        start_date (str): Sensibly formatted date string (interpretted by pd)
        article_ids (set): Set of article IDs to skip.
        test (bool): Running in test mode?
        flush_every (int): Sets the number of articles retrieved between log messages.
    Returns:
        articles (:obj:`list` of :obj:`dict`): List of articles from MAG, formatted a-la-arxiv.
    """
    logging.info(f"Getting MAG data for '{xiv}'")
    articles = []
    article_ids = article_ids.copy()
    for row in get_magrxiv_articles(xiv, api_key, start_date=start_date):
        # Don't recollect if already specified
        if row['id'] in article_ids:
            continue
        article_ids.add(row['id'])
        articles.append(row)
        # Log and break (test-mode) if hit the flush threshold
        if not len(articles) % flush_every:
            logging.info(f"Processed {len(articles)} articles")
            if test:
                break
    return articles

class CollectMagrxivTask(luigi.Task):
    '''Collect bio/medrxiv articles from MAG and dump the
    data in the MySQL server under the arxiv table.

    Args:
        date (datetime): Datetime used to label the outputs
        db_config_env (str): environmental variable pointing to the db config file
        db_config_path (str): The output database configuration
        insert_batch_size (int): Number of records to insert into the database at once
        articles_from_date (str): Earliest possible date considered to collect articles.
        dont_recollect (bool): If True, article which are already in the DB will not be recollected.
    '''
    date = luigi.DateParameter()
    dont_recollect = luigi.BoolParameter(default=False)
    routine_id = luigi.Parameter()
    test = luigi.BoolParameter(default=True)
    xivs = luigi.ListParameter(default=['medrxiv', 'biorxiv'])
    db_config_env = luigi.Parameter()
    db_config_path = luigi.Parameter()
    insert_batch_size = luigi.IntParameter(default=500)
    articles_from_date = luigi.Parameter()

    def output(self):
        '''Points to the output database engine'''
        db_config = misctools.get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "arXlive <dummy>"  # Note, not a real table
        update_id = "MagrxivCollect_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        # Variable preparation
        api_key = misctools.get_config('mag.config', "mag")['subscription_key']
        articles_from_date = pd.Timestamp(self.articles_from_date).to_pydatetime()
        # Database setup and connection
        database = 'dev' if self.test else 'production'
        logging.info(f"Using {database} database")
        engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)
        with db_session(engine) as session:
            article_ids = set() if not self.dont_recollect else get_article_ids(session, self.xivs)
            articles = get_articles(self.xivs, api_key, articles_from_date,
                                    article_ids, self.test)
            insert_data(self.db_config_env, "mysqldb", database, Base, Article,
                        articles, low_memory=True)
        # Mark as done
        self.output().touch()


class MagrxivRootTask(luigi.WrapperTask):
    '''A dummy root task, which collects the database configurations
    and executes the central task.

    Args:
        date (datetime): Date used to label the outputs
        db_config_path (str): Path to the MySQL database configuration
        production (bool): Flag indicating whether running in testing
                           mode (False, default), or production mode (True).
        drop_and_recreate (bool): If in test mode, allows dropping the dev index from the ES database.

    '''
    date = luigi.DateParameter(default=dt.today())
    db_config_path = luigi.Parameter(default="mysqldb.config")
    production = luigi.BoolParameter(default=False)
    drop_and_recreate = luigi.BoolParameter(default=False)
    articles_from_date = luigi.Parameter(default=None)
    insert_batch_size = luigi.IntParameter(default=500)

    def requires(self):
        routine_id = "{}-{}".format(self.date, self.production)
        logging.getLogger().setLevel(logging.INFO)
        articles_from_date = dt.strftime(dt.now() - timedelta(days=11), '%d %B %Y')
        if self.production:
            articles_from_date = '1 January 2010'
        yield CollectMagrxivTask(date=self.date,
                                 routine_id=routine_id,
                                 test=not self.production,
                                 db_config_env='MYSQLDB',
                                 db_config_path=self.db_config_path,
                                 articles_from_date=articles_from_date)
