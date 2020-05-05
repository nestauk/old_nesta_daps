'''
Collection task
===============

Luigi routine to collect new data from the arXiv api and load it to MySQL.
'''
from datetime import datetime
import luigi
import logging

from nesta.packages.arxiv.collect_arxiv import add_new_articles, retrieve_all_arxiv_rows, update_existing_articles
from nesta.packages.misc_utils.batches import BatchWriter
from nesta.core.orms.arxiv_orm import Article, Category
from nesta.core.orms.orm_utils import get_mysql_engine, db_session
from nesta.core.luigihacks import misctools
from nesta.core.luigihacks.mysqldb import MySqlTarget


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

        with db_session(self.engine) as session:
            # create lookup for categories (less than 200) and set of article ids
            all_categories_lookup = {cat.id: cat for cat in session.query(Category).all()}
            logging.info(f"{len(all_categories_lookup)} existing categories")
            all_article_ids = {article.id for article in session.query(Article.id).all()}
            logging.info(f"{len(all_article_ids)} existing articles")
            already_updated = {article.id: article.updated for article
                               in (session.query(Article)
                                   .filter(Article.updated >= self.articles_from_date)
                                   .all())}
            logging.info(f"{len(already_updated)} records exist in the database with a date on or after the update date.")

            new_count = 0
            existing_count = 0
            new_articles_batch = BatchWriter(self.insert_batch_size,
                                             add_new_articles,
                                             session)
            existing_articles_batch = BatchWriter(self.insert_batch_size,
                                                  update_existing_articles,
                                                  self.engine)

            # retrieve and process, while inserting any missing categories
            for row in retrieve_all_arxiv_rows(**{'from': self.articles_from_date}):
                try:
                    # update only newer data
                    if row['updated'] <= already_updated[row['id']]:
                        continue
                except KeyError:
                    pass

                # check for missing categories
                for cat in row.get('categories', []):
                    try:
                        cat = all_categories_lookup[cat]
                    except KeyError:
                        logging.warning(f"Missing category: '{cat}' for article {row['id']}.  Adding to Category table")
                        new_cat = Category(id=cat)
                        session.add(new_cat)
                        session.commit()
                        all_categories_lookup.update({cat: ''})

                # create new Article and append to batch
                if row['id'] not in all_article_ids:
                    # convert category ids to Category objects
                    row['categories'] = [all_categories_lookup[cat]
                                         for cat in row.get('categories', [])]
                    new_articles_batch.append(Article(**row))
                    new_count += 1
                else:
                    # append to existing articles batch
                    existing_articles_batch.append(row)
                    existing_count += 1

                count = new_count + existing_count
                if not count % 1000:
                    logging.info(f"Processed {count} articles")
                if self.test and count == 1600:
                    logging.warning("Limiting to 1600 rows while in test mode")
                    break

            # insert any remaining new and existing articles
            logging.info("Processing final batches")
            if new_articles_batch:
                new_articles_batch.write()
            if existing_articles_batch:
                existing_articles_batch.write()

        logging.info(f"Total {new_count} new articles added")
        logging.info(f"Total {existing_count} existing articles updated")

        # mark as done
        logging.warning("Task complete")
        self.output().touch()
