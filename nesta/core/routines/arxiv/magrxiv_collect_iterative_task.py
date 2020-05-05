'''
Collection task
===============

Luigi routine to collect new data from the arXiv api and load it to MySQL.
'''
from datetime import datetime
import luigi
import logging
import pandas as pd

from nesta.packages.magrxiv.collect_magrxiv import get_magrxiv_articles
from nesta.core.orms.arxiv_orm import Article, Base
from nesta.core.orms.orm_utils import get_mysql_engine, db_session, insert_data
from nesta.core.luigihacks import misctools
from nesta.core.luigihacks.mysqldb import MySqlTarget


class CollectMagrxivTask(luigi.Task):
    '''Collect bio/medrxiv articles from MAG and dump the
    data in the MySQL server under the arxiv table.

    Args:
        date (datetime): Datetime used to label the outputs
        db_config_env (str): environmental variable pointing to the db config file
        db_config_path (str): The output database configuration
        insert_batch_size (int): Number of records to insert into the database at once
        articles_from_date (str): Earliest possible date considered to collect articles.
    '''
    date = luigi.DateParameter()
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
        # database setup
        database = 'dev' if self.test else 'production'
        logging.warning(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)

        mag_config = misctools.get_config('mag.config', "mag")
        api_key = mag_config['subscription_key']

        articles_from_date = pd.Timestamp(self.articles_from_date).to_pydatetime()
        xiv_filter = Article.article_source.in_(self.xivs)
        with db_session(self.engine) as session:
            all_article_ids = {article.id for article in
                               session.query(Article.id).filter(xiv_filter).all()}
            logging.info(f"{len(all_article_ids)} existing articles")

            # retrieve and process, while inserting any missing categories
            articles = []
            for xiv in self.xivs:
                logging.info(f"Getting MAG data for '{xiv}'")
                for row in get_magrxiv_articles(xiv, api_key, start_date=articles_from_date):
                    if row['id'] in all_article_ids:
                        continue
                    all_article_ids.add(row['id'])
                    articles.append(row)
                    if not len(articles) % 1000:
                        logging.info(f"Processed {len(articles)} articles")
                        if self.test:
                            break
                if self.test and len(articles) > 1000:
                    logging.warning(f"Limiting to {len(articles)} rows while in test mode")
                    break
                logging.info(f"Retrieved {len(articles)} articles so far")
                    
            # insert any remaining new and existing articles            
            logging.info("Inserting data")    
            print(len(articles), len(set((row['id'] for row in articles))))
            objs = insert_data(self.db_config_env, "mysqldb", database,
                               Base, Article, articles, low_memory=True)
            logging.info(f"\t\tInserted {len(objs)}")

        # mark as done
        logging.warning("Task complete")
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
    date = luigi.DateParameter(default=datetime.today())
    db_config_path = luigi.Parameter(default="mysqldb.config")
    production = luigi.BoolParameter(default=False)
    drop_and_recreate = luigi.BoolParameter(default=False)
    articles_from_date = luigi.Parameter(default=None)
    insert_batch_size = luigi.IntParameter(default=500)
    
    def requires(self):
        routine_id = "{}-{}".format(self.date, self.production)
        logging.getLogger().setLevel(logging.INFO)
        yield CollectMagrxivTask(date=self.date,
                                 routine_id=routine_id,
                                 test=not self.production,
                                 db_config_env='MYSQLDB',
                                 db_config_path=self.db_config_path,
                                 articles_from_date='1 April 2020')
