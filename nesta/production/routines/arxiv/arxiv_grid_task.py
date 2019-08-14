from fuzzywuzzy import fuzz
import luigi
import logging

from nesta.production.routines.arxiv.arxiv_mag_sparql_task import MagSparqlTask
from nesta.packages.arxiv.collect_arxiv import add_article_institutes, create_article_institute_links, update_existing_articles
from nesta.packages.grid.grid import ComboFuzzer, grid_name_lookup
from nesta.packages.misc_utils.batches import BatchWriter
from nesta.production.orms.arxiv_orm import Base, Article
from nesta.production.orms.grid_orm import Institute
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
        yield MagSparqlTask(date=self.date,
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
        logging.info(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)
        Base.metadata.create_all(self.engine)

        article_institute_batcher = BatchWriter(self.insert_batch_size,
                                                add_article_institutes,
                                                self.engine)
        match_attempted_batcher = BatchWriter(self.insert_batch_size,
                                              update_existing_articles,
                                              self.engine)

        fuzzer = ComboFuzzer([fuzz.token_sort_ratio, fuzz.partial_ratio],
                             store_history=True)

        # extract lookup of GRID institute names to ids - seems to be OK to hold in memory
        institute_name_id_lookup = grid_name_lookup(self.engine)

        with db_session(self.engine) as session:
            # used to check GRID ids from MAG are valid (they are not all...)
            all_grid_ids = {i.id for i in session.query(Institute.id).all()}
            logging.info(f"{len(all_grid_ids)} institutes in GRID")

            article_query = (session
                             .query(Article.id, Article.mag_authors)
                             .filter(Article.institute_match_attempted.is_(False)
                                     & ~Article.institutes.any()
                                     & Article.mag_authors.isnot(None)))
            total = article_query.count()
            logging.info(f"Total articles with authors and no institutes links: {total}")

            logging.debug("Starting the matching process")
            articles = article_query.all()

        for count, article in enumerate(articles, start=1):
            article_institute_links = []
            for author in article.mag_authors:
                # prevent duplicates when a mixture of institute aliases are used in the same article
                existing_article_institute_ids = {link['institute_id']
                                                  for link in article_institute_links}

                # extract and validate grid_id
                try:
                    extracted_grid_id = author['affiliation_grid_id']
                except KeyError:
                    pass
                else:
                    # check grid id is valid
                    if (extracted_grid_id in all_grid_ids
                            and extracted_grid_id not in existing_article_institute_ids):
                        links = create_article_institute_links(article_id=article.id,
                                                               institute_ids=[extracted_grid_id],
                                                               score=1)
                        article_institute_links.extend(links)
                        logging.debug(f"Used grid_id: {extracted_grid_id}")
                        continue

                # extract author affiliation
                try:
                    affiliation = author['author_affiliation']
                except KeyError:
                    # no grid id or affiliation for this author
                    logging.debug(f"No affiliation found in: {author}")
                    continue

                # look for an exact match on affiliation name
                try:
                    institute_ids = institute_name_id_lookup[affiliation]
                except KeyError:
                    pass
                else:
                    institute_ids = set(institute_ids) - existing_article_institute_ids
                    links = create_article_institute_links(article_id=article.id,
                                                           institute_ids=institute_ids,
                                                           score=1)
                    article_institute_links.extend(links)
                    logging.debug(f"Found an exact match for: {affiliation}")
                    continue

                # fuzzy matching
                try:
                    match, score = fuzzer.fuzzy_match_one(affiliation,
                                                          institute_name_id_lookup.keys())
                except KeyError:
                    # failed fuzzy match
                    logging.debug(f"Failed fuzzy match: {affiliation}")
                else:
                    institute_ids = institute_name_id_lookup[match]
                    institute_ids = set(institute_ids) - existing_article_institute_ids
                    links = create_article_institute_links(article_id=article.id,
                                                           institute_ids=institute_ids,
                                                           score=score)
                    article_institute_links.extend(links)
                    logging.debug(f"Found a fuzzy match: {affiliation}  {score}  {match}")

            # add links for this article to the batch queue
            article_institute_batcher.extend(article_institute_links)
            # mark that matching has been attempted for this article
            match_attempted_batcher.append(dict(id=article.id,
                                                institute_match_attempted=True))

            if not count % 100:
                logging.info(f"{count} processed articles from {total} : {(count / total) * 100:.1f}%")

            if self.test and count == 50:
                logging.warning("Exiting after 50 articles in test mode")
                logging.debug(article_institute_batcher)
                break

        # pick up any left over in the batches
        if article_institute_batcher:
            article_institute_batcher.write()
        if match_attempted_batcher:
            match_attempted_batcher.write()

        logging.info("All articles processed")
        logging.info(f"Total successful fuzzy matches for institute names: {len(fuzzer.successful_fuzzy_matches)}")
        logging.info(f"Total failed fuzzy matches for institute names{len(fuzzer.failed_fuzzy_matches): }")

        # mark as done
        logging.info("Task complete")
        self.output().touch()
