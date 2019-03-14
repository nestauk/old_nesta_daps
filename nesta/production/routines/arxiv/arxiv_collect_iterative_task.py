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


class CollectNewTask(luigi.Task):
    '''Collect new data from the arXiv api and dump the
    data in the MySQL server.

    Args:
        date (datetime): Datetime used to label the outputs
        _routine_id (str): String used to label the AWS task
        db_config_env (str): environmental variable pointing to the db config file
        db_config_path (str): The output database configuration
        insert_batch_size (int): number of records to insert into the database at once
        articles_from_date (str): new and updated articles from this data will be retrieved
                                  Must be in YYYY-MM-DD format
    '''
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    test = luigi.BoolParameter(default=True)
    db_config_env = luigi.Parameter()
    db_config_path = luigi.Parameter()
    insert_batch_size = luigi.IntParameter(default=500)
    articles_from_date = luigi.Parameter()

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

        with db_session(self.engine) as session:
            all_categories = session.query(Category.id).all()
            all_categories = {cat_id for (cat_id, ) in all_categories}

        # process data
        articles = []
        article_cats = []
        for count, row in enumerate(retrieve_all_arxiv_rows(**{'from': self.articles_from_date}), 1):
            categories = row.pop('categories', [])
            articles.append(row)
            for cat in categories:
                if cat not in all_categories:
                    logging.warning(f"missing category: '{cat}' for article {row['id']}.  Adding to Category table")
                    with db_session(self.engine) as session:
                        session.add(Category(id=cat))
                    all_categories.add(cat)
                article_cats.append(dict(article_id=row['id'], category_id=cat))
            if self.test and count == 2000:
                logging.warning("limiting to 2000 rows while in test mode")
                break

        logging.info(f"Total articles: {len(articles)}")
        inserted_articles, existing_articles, failed_articles = insert_data(
                                                    self.db_config_env, "mysqldb", database,
                                                    Base, Article, articles,
                                                    return_non_inserted=True)
        logging.info(f"Inserted {len(inserted_articles)} new articles")
        if len(failed_articles) > 0:
            raise ValueError(f"Articles could not be inserted: {failed_articles}")

        # insert existing articles using a different method
        logging.info(f"{len(existing_articles)} existing articles")
        for batch in split_batches(existing_articles, self.insert_batch_size):
            with db_session(self.engine) as session:
                session.bulk_update_mappings(Article, existing_articles)
            logging.info(f"{count} batch{'es' if count > 1 else ''} written to db")

        logging.info(f"Total article categories: {len(article_cats)}")
        inserted_article_cats, _, failed_article_cats = insert_data(
                                                    self.db_config_env, "mysqldb", database,
                                                    Base, ArticleCategory, article_cats,
                                                    return_non_inserted=True)
        logging.info(f"Inserted {len(inserted_article_cats)} new article categories")
        if len(failed_article_cats) > 0:
            raise ValueError(f"Categories could not be inserted: {failed_article_cats}")

        # mark as done
        logging.warning("Task complete")
        self.output().touch()
