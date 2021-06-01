"""
arXiv enriched with MAG (API)
=============================

Luigi routine to query the Microsoft Academic Graph for additional data and append it to
the exiting data in the database.
"""
from datetime import date
from datetime import datetime as dt
from datetime import timedelta
import luigi
import logging
import pprint

from nesta.core.routines.arxiv.cord_collect_task import CollectCordTask
from nesta.core.routines.arxiv.arxiv_iterative_date_task import DateTask
from nesta.core.routines.arxiv.magrxiv_collect_iterative_task import CollectMagrxivTask
from nesta.packages.arxiv.collect_arxiv import BatchedTitles, update_existing_articles
from nesta.packages.misc_utils.batches import BatchWriter
from nesta.packages.mag.query_mag_api import (
    build_expr,
    query_mag_api,
    dedupe_entities,
    update_field_of_study_ids,
)
from nesta.core.orms.arxiv_orm import Base, Article
from nesta.core.orms.mag_orm import FieldOfStudy
from nesta.core.orms.orm_utils import get_mysql_engine, db_session
from nesta.core.luigihacks import misctools
from nesta.core.luigihacks.mysqldb import MySqlTarget


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
    article_source = luigi.Parameter(default=None)

    def output(self):
        """Points to the output database engine"""
        db_config = misctools.get_config(self.db_config_path, "mysqldb")
        db_config["database"] = "dev" if self.test else "production"
        db_config["table"] = "arXlive <dummy>"  # Note, not a real table
        update_id = "ArxivQueryMag_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def requires(self):
        kwargs = dict(
            date=self.date,
            db_config_path=self.db_config_path,
            db_config_env=self.db_config_env,
            test=self.test,
        )
        article_source = self.article_source
        if (article_source is None) or (article_source == "arxiv"):
            yield DateTask(
                _routine_id=self._routine_id,
                articles_from_date=self.articles_from_date,
                insert_batch_size=self.insert_batch_size,
                **kwargs,
            )
        if (article_source is None) or (article_source == "cord"):
            yield CollectCordTask(routine_id=self._routine_id, **kwargs)
        if article_source is None:
            # Start collection from Jan 2010 unless in test mode
            articles_from_date = "1 January 2010"
            if self.test:  # 11 days ago for test
                articles_from_date = dt.strftime(
                    dt.now() - timedelta(days=11), "%d %B %Y"
                )
            yield CollectMagrxivTask(
                routine_id=self._routine_id,
                articles_from_date=articles_from_date,
                insert_batch_size=self.insert_batch_size,
                **kwargs,
            )

    def run(self):
        pp = pprint.PrettyPrinter(indent=4, width=100)
        mag_config = misctools.get_config(self.mag_config_path, "mag")
        mag_subscription_key = mag_config["subscription_key"]

        # database setup
        database = "dev" if self.test else "production"
        logging.warning(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, "mysqldb", database)
        Base.metadata.create_all(self.engine)

        with db_session(self.engine) as session:
            paper_fields = [
                "Id",
                "Ti",
                "F.FId",
                "CC",
                "AA.AuN",
                "AA.AuId",
                "AA.AfN",
                "AA.AfId",
                "AA.S",
            ]

            author_mapping = {
                "AuN": "author_name",
                "AuId": "author_id",
                "AfN": "author_affiliation",
                "AfId": "author_affiliation_id",
                "S": "author_order",
            }

            field_mapping = {
                "Id": "mag_id",
                "Ti": "title",
                "F": "fields_of_study",
                "AA": "mag_authors",
                "CC": "citation_count",
                "logprob": "mag_match_prob",
            }

            logging.info("Querying database for articles without fields of study")
            arxiv_ids_to_process = {
                a.id
                for a in (
                    session.query(Article).filter(~Article.fields_of_study.any()).all()
                )
            }
            total_arxiv_ids_to_process = len(arxiv_ids_to_process)
            logging.info(f"{total_arxiv_ids_to_process} articles to process")

            all_articles_to_update = BatchWriter(
                self.insert_batch_size, update_existing_articles, self.engine
            )

            batched_titles = BatchedTitles(arxiv_ids_to_process, 10000, session)
            batch_field_of_study_ids = set()

            for count, expr in enumerate(build_expr(batched_titles, "Ti"), 1):
                logging.debug(pp.pformat(expr))
                expr_length = len(expr.split(","))
                logging.info(f"Querying MAG for {expr_length} titles")
                total_arxiv_ids_to_process -= expr_length
                batch_data = query_mag_api(expr, paper_fields, mag_subscription_key)
                logging.debug(pp.pformat(batch_data))

                returned_entities = batch_data["entities"]
                logging.info(
                    f"{len(returned_entities)} entities returned from MAG (potentially including duplicates)"
                )

                # dedupe response keeping the entity with the highest logprob
                deduped_mag_ids = dedupe_entities(returned_entities)
                logging.info(f"{len(deduped_mag_ids)} entities after deduplication")

                missing_articles = expr_length - len(deduped_mag_ids)
                if missing_articles != 0:
                    logging.info(f"{missing_articles} titles not found in MAG")

                batch_article_data = []

                for row in returned_entities:
                    # exclude duplicate titles
                    if row["Id"] not in deduped_mag_ids:
                        continue

                    # renaming and reformatting
                    for code, description in field_mapping.items():
                        try:
                            row[description] = row.pop(code)
                        except KeyError:
                            pass

                    for author in row.get("mag_authors", []):
                        for code, description in author_mapping.items():
                            try:
                                author[description] = author.pop(code)
                            except KeyError:
                                pass

                    if row.get("citation_count", None) is not None:
                        row["citation_count_updated"] = date.today()

                    # reformat fos_ids out of dictionaries
                    try:
                        row["fields_of_study"] = {
                            f["FId"] for f in row.pop("fields_of_study")
                        }
                    except KeyError:
                        row["fields_of_study"] = []
                    batch_field_of_study_ids.update(row["fields_of_study"])

                    # get list of ids which share the same title
                    try:
                        matching_articles = batched_titles[row["title"]]
                    except KeyError:
                        logging.warning(
                            f"Returned title not found in original data: {row['title']}"
                        )
                        continue

                    # drop unnecessary fields
                    for f in ["prob", "title"]:
                        del row[f]

                    # add each matching article for this title to the batch
                    for article_id in matching_articles:
                        batch_article_data.append({**row, "id": article_id})

                # check fields of study are in database
                batch_field_of_study_ids = {
                    fos_id
                    for article in batch_article_data
                    for fos_id in article["fields_of_study"]
                }
                logging.debug("Checking fields of study exist in db")
                found_fos_ids = {
                    fos.id
                    for fos in (
                        session.query(FieldOfStudy)
                        .filter(FieldOfStudy.id.in_(batch_field_of_study_ids))
                        .all()
                    )
                }

                missing_fos_ids = batch_field_of_study_ids - found_fos_ids
                if missing_fos_ids:
                    #  query mag for details if not found
                    update_field_of_study_ids(
                        mag_subscription_key, session, missing_fos_ids
                    )

                # add this batch to the queue
                all_articles_to_update.extend(batch_article_data)

                logging.info(
                    f"Batch {count} done. {total_arxiv_ids_to_process} articles left to process"
                )
                if self.test and count == 10:
                    logging.warning("Exiting after 10 batches in test mode")
                    break

            # pick up any left over in the batch
            if all_articles_to_update:
                all_articles_to_update.write()

        # mark as done
        logging.warning("Task complete")
        self.output().touch()
