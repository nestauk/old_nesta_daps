from collections import defaultdict
import luigi
import logging

# from arxiv_mag_task import QueryMagTask
from nesta.packages.arxiv.collect_arxiv import update_existing_articles
from nesta.packages.misc_utils.batches import BatchWriter
from nesta.production.orms.arxiv_orm import Base, Article
from nesta.production.orms.grid_orm import Institute, Alias
from nesta.production.orms.orm_utils import get_mysql_engine, db_session
from nesta.production.luigihacks import misctools
from nesta.production.luigihacks.mysqldb import MySqlTarget


class GridTask(luigi.Task):
    """Join arxiv articles with GRID data for institute addresses and geocoding.

    Args:
        date (datetime): Datetime used to label the outputs
        _routine_id (str): String used to label the AWS task
        db_config_env (str): environmental variable pointing to the db config file
        db_config_path (str): The output database configuration
        mag_config_path (str): Microsoft Academic Graph Api key configuration path
        insert_batch_size (int): number of records to insert into the database at once
                                 (not used in this task but passed down to others)
        articles_from_date (str): new and updated articles from this date will be
                                  retrieved. Must be in YYYY-MM-DD format
                                  (not used in this task but passed down to others)
    """
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    test = luigi.BoolParameter(default=True)
    db_config_env = luigi.Parameter()
    db_config_path = luigi.Parameter()
    mag_config_path = luigi.Parameter()
    insert_batch_size = luigi.IntParameter(default=500)
    articles_from_date = luigi.Parameter()

    def output(self):
        '''Points to the output database engine'''
        db_config = misctools.get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "arXlive <dummy>"  # Note, not a real table
        update_id = "ArxivGrid_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def requires(self):
        yield QueryMagTask(date=self.date,
                           _routine_id=self._routine_id,
                           db_config_path=self.db_config_path,
                           db_config_env=self.db_config_env,
                           mag_config_path=self.mag_config_path,
                           test=self.test,
                           articles_from_date=self.articles_from_date,
                           insert_batch_size=self.insert_batch_size)

    def run(self):
        # database setup
        database = 'dev' if self.test else 'production'
        logging.warning(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)
        Base.metadata.create_all(self.engine)

        with db_session(self.engine) as session:
            # extract affiliations for each article
            articles_to_process = defaultdict(set)
            for article in (session
                            .query(Article)
                            .filter(~Article.institutes.any() & Article.mag_authors.isnot(None))
                            .all()):
                for author in article.mag_authors:
                    try:
                        affiliation = author['author_affiliation']
                    except KeyError:
                        pass
                    else:
                        articles_to_process[article.id].add(affiliation)
            logging.info(f"{len(articles_to_process)} articles with affiliations")

            # extract GRID data
            institute_name_lookup = {}
            for institute in session.query(Institute).all():
                institute_name_lookup.update({institute.name.lower(): institute.id})

            for alias in session.query(Alias).all():
                institute_name_lookup.update({alias.alias.lower(): alias.grid_id})

            # look for exact matches
            matches = {}
            for id, affiliations in articles_to_process.items():
                for affiliation in affiliations:
                    try:
                        matches.update({id: institute_name_lookup[affiliation]})
                    except KeyError:
                        pass
            logging.info(f"Matched {len(matches)} on exact match")
            # *****add lower to the name searching for also?

                    



            # build lookup table of previous matches so they do not have to be recalculated
            # result needs to be a dict of {article_id: institute_id} for direct load to the association table
