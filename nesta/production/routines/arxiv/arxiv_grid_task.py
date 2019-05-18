from collections import defaultdict
from fuzzywuzzy import fuzz
from fuzzywuzzy import process as fuzzy_proc
import luigi
import logging
import re

from arxiv_mag_sparql_task import MagSparqlTask
from nesta.packages.arxiv.collect_arxiv import add_article_institutes
from nesta.packages.grid.grid import ComboFuzzer
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

        combo_fuzzer = ComboFuzzer([fuzz.token_sort_ratio, fuzz.partial_ratio])

        with db_session(self.engine) as session:
            # extract GRID data - seems to be OK to hold in memory
            institute_name_id_lookup = {institute.name.lower(): [institute.id]
                                        for institute in session.query(Institute).all()}
            logging.info(f"{len(institute_name_id_lookup)} institutes in GRID")

            for alias in session.query(Alias).all():
                institute_name_id_lookup.update({alias.alias.lower(): [alias.grid_id]})
            logging.info(f"{len(institute_name_id_lookup)} institutes after adding aliases")

            # look for institute names containing brackets: IBM (United Kingdom)
            with_country = defaultdict(list)
            for bracketed in (session
                              .query(Institute)
                              .filter(Institute.name.contains('(') & Institute.name.contains(')'))
                              .all()):
                found = re.match(r'(.*) \((.*)\)', bracketed.name)
                if found:
                    # combine all matches to a cleaned country name {IBM : [grid_id1, grid_id2]}
                    with_country[found.groups()[0]].append(bracketed.id)
            logging.info(f"{len(with_country)} institutes with country name in the title")

            # append cleaned names to the lookup table
            institute_name_id_lookup.update(with_country)
            logging.info(f"{len(institute_name_id_lookup)} total institute names in lookup")

            logging.debug("Starting the matching process")
            successful_fuzzy_matches = {}
            failed_fuzzy_matches = set()
            article_query = (session
                             .query(Article)
                             .filter(~Article.institutes.any() & Article.mag_authors.isnot(None)))
            total = article_query.count()
            logging.info(f"Total articles with authors and no institutes links: {total}")

            for count, article in enumerate(article_query.all(), start=1):
                article_institute_links = []
                for affiliation in {author.get('author_affiliation')
                                    for author in article.mag_authors
                                    if author.get('author_affiliation') is not None}:
                    institute_ids = []
                    try:
                        # look for an exact match
                        institute_ids = institute_name_id_lookup[affiliation]
                        score = 1
                        logging.debug(f"Found an exact match for: {affiliation}")
                    except KeyError:
                        if affiliation in failed_fuzzy_matches:
                            continue
                        # check previous fuzzy matches
                        match, score = successful_fuzzy_matches.get(affiliation, (None, None))
                        if not match:
                            # attempt a new fuzzy match
                            match, score = fuzzy_proc.extractOne(query=affiliation,
                                                                 choices=institute_name_id_lookup.keys(),
                                                                 scorer=combo_fuzzer.combo_fuzz)
                        if score < 0.85:  # <0.85 is definitely a bad match
                            logging.debug(f"Failed to find a match for: {affiliation}")
                            failed_fuzzy_matches.add(affiliation)
                        else:
                            successful_fuzzy_matches.update({affiliation: (match, score)})
                            institute_ids = institute_name_id_lookup[match]
                            logging.debug(f"Found a fuzzy match: {affiliation} {score} {match}")

                    # prevent duplicates when a mixture of institute aliases are used in the same article
                    found_institute_ids = {link['institute_id'] for link in article_institute_links}
                    # add an entry to the link table for each grid id (there will be multiple if the org is multinational)
                    article_institute_links.extend({'article_id': article.id,
                                                    'institute_id': institute_id,
                                                    'is_multinational': len(institute_ids) > 1,
                                                    'matching_score': float(score)}
                                                   for institute_id in institute_ids
                                                   if institute_id not in found_institute_ids)

                # add links for this article to the batch queue
                article_institute_batcher.extend(article_institute_links)

                if not count % 100:
                    logging.info(f"{count} processed articles from {total} : {int(count / total)}%")

                if self.test and count > 10:
                    logging.warning("Exiting after 10 articles in test mode")
                    logging.debug(article_institute_batcher)
                    break

        # pick up any left over in the batch
        if article_institute_batcher:
            article_institute_batcher.write()

        logging.info("Task complete")
        logging.info(f"Made {len(successful_fuzzy_matches)} fuzzy matches")
        logging.info(f"Failed to match {len(failed_fuzzy_matches)} institute names")

        # mark as done
        self.output().touch()
