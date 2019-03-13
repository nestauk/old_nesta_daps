'''
arXiv data collection and processing
====================================

Luigi routine to collect new data from the arXiv api and load it to MySQL.
'''

import luigi
import logging
from sqlalchemy.orm.exc import NoResultFound

from nesta.packages.arxiv.collect_arxiv import retrieve_all_arxiv_rows
from nesta.production.orms.arxiv_orm import Base, Article, ArticleCategory, Category
from nesta.production.orms.orm_utils import get_mysql_engine, insert_data, db_session
from nesta.production.luigihacks import misctools
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.packages.misc_utils.batches import split_batches

import boto3

S3 = boto3.resource('s3')
_BUCKET = S3.Bucket("nesta-production-intermediate")
DONE_KEYS = set(obj.key for obj in _BUCKET.objects.all())
BATCH_SIZE = 10000


class CollectNewTask(luigi.Task):
    '''Collect new data from the arXiv api and dump the
    data in the MySQL server.

    Args:
        date (datetime): Datetime used to label the outputs
        _routine_id (str): String used to label the AWS task
        db_config_path: (str) The output database configuration
    '''
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    db_config_env = luigi.Parameter()
    db_config_path = luigi.Parameter()
    insert_batch_size = luigi.IntParameter(default=500)
    from_date = luigi.Parameter()

    def output(self):
        '''Points to the output database engine'''
        db_config = misctools.get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "arxiv iterative <dummy>"  # Note, not a real table
        update_id = "ArxivCollectData_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        # database setup
        database = 'dev' if self.test else 'production'
        logging.warning(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)

        # process data
        articles = []
        article_cats = []
        for row in retrieve_all_arxiv_rows(**{'from': self.from_date}):
            with db_session(self.engine) as session:
                categories = row.pop('categories', [])
                articles.append(row)
                for cat in categories:
                    try:
                        session.query(Category).filter(Category.id == cat).one()
                    except NoResultFound:
                        logging.warning(f"missing category: '{cat}' for article {row['id']}.  Adding to Category table")
                        session.add(Category(id=cat))
                    article_cats.append(dict(article_id=row['id'], category_id=cat))

        logging.info(f"Total articles: {len(articles)}")
        inserted_articles, existing_articles, failed_articles = insert_data(
                                                    "BATCHPAR_config", "mysqldb", database,
                                                    Base, Article, articles,
                                                    return_non_inserted=True)
        logging.info(f"Inserted {len(inserted_articles)} new articles")
        if failed_articles is not None:
            raise ValueError(f"Articles could not be inserted: {failed_articles}")

        # insert existing articles using a different method
        logging.info(f"{len(existing_articles)} existing articles")
        for count, batch in enumerate(split_batches(existing_articles,
                                                    self.insert_batch_size), 1):
            with db_session(self.engine) as session:
                session.bulk_update_mappings(existing_articles, batch)
            logging.info(f"{count} batch{'es' if count > 1 else ''} written to db")
            if self.test and count > 1:
                logging.info("Breaking after 2 batches while in test mode")
                break

        logging.info(f"Total article categories: {len(article_cats)}")
        inserted_article_cats, _, failed_article_cats = insert_data(
                                                    "BATCHPAR_config", "mysqldb", database,
                                                    Base, ArticleCategory, article_cats,
                                                    return_non_inserted=True)
        logging.info(f"Inserted {len(inserted_article_cats)} new article categories")
        if failed_article_cats is not None:
            raise ValueError(f"Categories could not be inserted: {failed_article_cats}")

        # mark as done
        logging.warning("Task complete")
        self.output().touch()
