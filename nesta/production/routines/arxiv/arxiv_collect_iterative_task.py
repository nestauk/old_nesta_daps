'''
arXiv data collection and processing
====================================

Luigi routine to collect new data from the arXiv api and load it to MySQL.
'''

from datetime import datetime
import luigi
import logging
from sqlalchemy.orm import sessionmaker

from nesta.packages.arxiv.collect_arxiv import retrieve_all_arxiv_rows
from nesta.production.orms.arxiv_orm import Article, Category
from nesta.production.orms.orm_utils import get_mysql_engine
from nesta.production.luigihacks import misctools
from nesta.production.luigihacks.mysqldb import MySqlTarget


class CollectNewTask(luigi.Task):
    '''Collect new data from the arXiv api and dump the
    data in the MySQL server.

    Args:
        date (datetime): Datetime used to label the outputs
        _routine_id (str): String used to label the AWS task
        db_config_env (str): environmental variable pointing to the db config file
        db_config_path (str): The output database configuration
        insert_batch_size (int): number of records to insert into the database at once
        articles_from_date (str): new and updated articles from this date will be
                                  retrieved. Must be in YYYY-MM-DD format
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
        db_config["table"] = "arXlive <dummy>"  # Note, not a real table
        update_id = "ArxivIterativeCollect_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        try:
            datetime.strptime(self.articles_from_date, '%Y-%m-%d')
        except ValueError:
            raise ValueError(f"From date for articles is invalid or not in YYYY-MM-DD format: {self.articles_from_date}")
        # database setup
        database = 'dev' if self.test else 'production'
        logging.warning(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)
        Session = sessionmaker(self.engine)
        session = Session()

        # create lookup for categories (less than 200) and set of article ids
        all_categories_lookup = {cat.id: cat for cat in session.query(Category).all()}
        logging.info(f"{len(all_categories_lookup)} existing categories")
        all_article_ids = {article.id for article in session.query(Article.id).all()}
        logging.info(f"{len(all_article_ids)} existing articles")

        new_count = 0
        existing_count = 0
        new_articles_batch = []

        # retrieve and process, while inserting any missing categories
        for row_count, row in enumerate(retrieve_all_arxiv_rows(**{'from': self.articles_from_date}), 1):
            # swap category ids for Category objects
            categories = row.pop('categories', [])
            row['categories'] = []
            for cat in categories:
                try:
                    cat = all_categories_lookup[cat]
                except KeyError:
                    logging.warning(f"Missing category: '{cat}' for article {row['id']}.  Adding to Category table")
                    cat = Category(id=cat)
                    all_categories_lookup[cat.id] = cat
                row['categories'].append(cat)

            if row['id'] not in all_article_ids:
                # create new Article and append to batch
                new_articles_batch.append(Article(**row))
                new_count += 1
                if len(new_articles_batch) == self.insert_batch_size:
                    logging.debug("Inserting a batch of new Articles")
                    session.add_all(new_articles_batch)
                    session.commit()
                    new_articles_batch.clear()
            else:
                # lookup and update existing article
                logging.debug(f"Updating existing record: {row}")
                categories = row.pop('categories')  # prevent hashing error using .update with a list
                existing = session.query(Article).filter(Article.id == row.pop('id'))
                existing.update(row)
                existing.one().categories = categories
                session.commit()
                existing_count += 1

            if not row_count % 1000:
                logging.info(f"Processed {row_count} articles")
            if self.test and row_count == 1600:
                logging.warning("Limiting to 1600 rows while in test mode")
                break

        # insert any remaining new articles
        if len(new_articles_batch) > 0:
            session.add_all(new_articles_batch)
            session.commit()

        session.close()
        logging.info(f"Total {new_count} new articles added")
        logging.info(f"Total {existing_count} existing articles updated")

        # mark as done
        logging.warning("Task complete")
        self.output().touch()
