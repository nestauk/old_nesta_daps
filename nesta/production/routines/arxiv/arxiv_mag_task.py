"""
arXiv data collection and processing
====================================

Luigi routine to query the Microsoft Academic Graph for additional data and append it to
the exiting data in the database.
"""
from collections import defaultdict
from datetime import date
import luigi
import logging
import pprint

from arxiv_iterative_date_task import DateTask
from nesta.packages.arxiv.collect_arxiv import batched_titles
from nesta.packages.mag.query_mag import build_expr, query_mag_api, query_fields_of_study, write_fields_of_study_to_db
from nesta.production.orms.arxiv_orm import Base, Article, ArticleFieldsOfStudy
from nesta.production.orms.mag_orm import FieldOfStudy
from nesta.production.orms.orm_utils import get_mysql_engine, db_session
from nesta.production.luigihacks import misctools
from nesta.production.luigihacks.mysqldb import MySqlTarget


class QueryMagTask(luigi.Task):
    """Query the MAG for additional data to append to the arxiv articles,
       primarily the fields of study.

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
        update_id = "ArxivQueryMag_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def requires(self):
        yield DateTask(date=self.date,
                       _routine_id=self._routine_id,
                       db_config_path=self.db_config_path,
                       db_config_env=self.db_config_env,
                       test=self.test,
                       articles_from_date=self.articles_from_date)

    def run(self):
        pp = pprint.PrettyPrinter(indent=4, width=100)

        # database setup
        database = 'dev' if self.test else 'production'
        logging.warning(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)
        Base.metadata.create_all(self.engine)

        mag_config = misctools.get_config(self.mag_config_path, 'mag')
        mag_subscription_key = mag_config['subscription_key']

        # extract article ids without fields of study and existing fields of study ids
        with db_session(self.engine) as session:
            arxiv_ids_with_fos = (session
                                  .query(ArticleFieldsOfStudy.article_id)
                                  .all())
            arxiv_ids_with_fos = {id for (id, ) in arxiv_ids_with_fos}
            logging.info(f"{len(arxiv_ids_with_fos)} articles already processed")

            arxiv_ids_to_process = (session
                                    .query(Article.id)
                                    .filter(~Article.id.in_(arxiv_ids_with_fos))
                                    .all())

            all_fos_ids = session.query(FieldOfStudy.id).all()

        arxiv_ids_to_process = {id for (id, ) in arxiv_ids_to_process}
        logging.info(f"{len(arxiv_ids_to_process)} articles to process")
        all_fos_ids = {id for (id, ) in all_fos_ids}
        logging.info(f"{len(all_fos_ids)} fields of study in the database")

        paper_fields = ["Id", "Ti", "F.FId", "CC",
                        "AA.AuN", "AA.AuId", "AA.AfN", "AA.AfId", "AA.S"]

        author_mapping = {'AuN': 'author_name',
                          'AuId': 'author_id',
                          'AfN': 'author_affiliation',
                          'AfId': 'author_affiliation_id',
                          'S': 'author_order'}

        field_mapping = {'Id': 'mag_id',
                         'Ti': 'title',
                         'F': 'field_of_study_ids',
                         'AA': 'mag_authors',
                         'CC': 'citation_count',
                         'logprob': 'mag_match_prob'}

        title_id_lookup = defaultdict(list)

        for count, expr in enumerate(build_expr(batched_titles(arxiv_ids_to_process,
                                                               title_id_lookup,
                                                               10000,
                                                               self.engine), 'Ti'), 1):
            logging.debug(pp.pprint(expr))
            expr_length = len(expr.split(','))
            logging.info(f"Querying MAG for {expr_length} titles")
            batch_data = query_mag_api(expr, paper_fields, mag_subscription_key)
            logging.debug(pp.pprint(batch_data))

            returned_entities = batch_data['entities']

            logging.info(f"{len(returned_entities)} entities returned from MAG (potentially including duplicates)")

            # dedupe response keeping the entity with the highest logprob
            titles = defaultdict(dict)
            for row in returned_entities:
                titles[row['Ti']].update({row['Id']: row['logprob']})

            logging.info(f"{len(titles)} entities after deduplication")
            deduped_ids = set()
            for title in titles.values():
                deduped_ids.add(sorted(title, key=title.get, reverse=True)[0])

            missing_articles = expr_length - len(titles)
            if missing_articles != 0:
                logging.info(f"{missing_articles} titles not found in MAG")

            batch_missing_fos = set()
            batch_article_fos_links = []
            batch_article_data = []

            for row in returned_entities:
                # exclude duplicates
                if row['Id'] not in deduped_ids:
                    continue

                # renaming and reformatting
                for code, description in field_mapping.items():
                    try:
                        row[description] = row.pop(code)
                    except KeyError:
                        pass

                for author in row['mag_authors']:
                    for code, description in author_mapping.items():
                        try:
                            author[description] = author.pop(code)
                        except KeyError:
                            pass

                if row.get('citation_count', None) is not None:
                    row['citation_count_updated'] = date.today()

                # reformat fos_ids out of a list of dictionaries
                field_of_study_ids = {f['FId'] for f in row.pop('field_of_study_ids')}

                # lookup list of ids (there are duplicates by title in arxiv)
                row_article_ids = title_id_lookup[row['title']]

                # drop unnecessary fields
                for f in ['prob', 'title']:
                    del row[f]

                # build new article data, fos links and missing fields of study
                # ready for load to db
                for article_id in row_article_ids:
                    batch_article_data.append({**row, 'id': article_id})
                    batch_article_fos_links.extend({'article_id': article_id, 'fos_id': id}
                                                   for id in field_of_study_ids)

                missing_fos_ids = set(field_of_study_ids) - all_fos_ids
                if missing_fos_ids:
                    logging.warning(f"Missing field of study ids {missing_fos_ids} for {row_article_ids}")
                    batch_missing_fos.update(missing_fos_ids)

            # write the batch to db, starting with any missing fields of study
            if batch_missing_fos:
                logging.info(f"Querying MAG for {len(batch_missing_fos)} missing fields of study")
                batch_found_fos = query_fields_of_study(mag_subscription_key,
                                                        ids=batch_missing_fos)
                logging.info(f"Found {len(batch_found_fos)} new fields of study")
                write_fields_of_study_to_db(batch_found_fos, self.engine)
                all_fos_ids.update(fos['id'] for fos in batch_found_fos)

            # update a batch of articles in the db
            logging.info(f"Updating {len(batch_article_data)} articles")
            logging.debug(pp.pprint(batch_article_data))
            with db_session(self.engine) as session:
                session.bulk_update_mappings(Article, batch_article_data)

            # write a batch of article/fields of study to the link table
            logging.info(f"Inserting {len(batch_article_fos_links)} article-field of study links")
            logging.debug(pp.pprint(batch_article_fos_links))
            with db_session(self.engine) as session:
                session.bulk_insert_mappings(ArticleFieldsOfStudy, batch_article_fos_links)

            logging.info(f"Batch {count} done")
            if count == 2:
                logging.warning("Exiting after 2 batches in test mode")
                break

        # mark as done
        logging.warning("Task complete")
        self.output().touch()
