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

from nesta.packages.arxiv.collect_arxiv import batched_titles
from nesta.packages.mag.query_mag import build_expr, query_mag_api
from nesta.production.orms.arxiv_orm import Base, Article, ArticleFieldsOfStudy
from nesta.production.orms.mag_orm import FieldOfStudy
from nesta.production.orms.orm_utils import get_mysql_engine, insert_data, db_session
from nesta.production.luigihacks import misctools
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.packages.misc_utils.batches import split_batches


class QueryMagTask(luigi.Task):
    """Query the MAG for additional data to append to the arxiv articles,
       primarily the fields of study.

    Args:
        date (datetime): Datetime used to label the outputs
        _routine_id (str): String used to label the AWS task
        db_config_env (str): environmental variable pointing to the db config file
        db_config_path (str): The output database configuration
        insert_batch_size (int): number of records to insert into the database at once
        articles_from_date (str): new and updated articles from this date will be
                                  retrieved. Must be in YYYY-MM-DD format
    """
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    test = luigi.BoolParameter(default=True)
    db_config_env = luigi.Parameter()
    db_config_path = luigi.Parameter()
    mag_config_path = luigi.Parameter()
    insert_batch_size = luigi.IntParameter(default=500)

    def output(self):
        '''Points to the output database engine'''
        db_config = misctools.get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "arXlive <dummy>"  # Note, not a real table
        update_id = "ArxivQueryMag_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        # database setup
        database = 'dev' if self.test else 'production'
        logging.warning(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)

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
        logging.info(f"{len(all_fos_ids)} fields of service in the database")

        # retrieve and process, while inserting any missing categories
        # article_fos = []  # article_id, fos_id
        # missing_fos = []

        paper_fields = ["Id", "Ti", "F.FId", "CC",
                        "AA.AuN", "AA.AuId", "AA.AfN", "AA.AfId", "AA.S"]

        author_mapping = {'AuN': 'author_name',
                          'AuId': 'author_id',
                          'AfN': 'author_affiliation',
                          'AfId': 'author_affiliation_id',
                          'S': 'author_order'}

        field_mapping = {"Id": 'id',
                         "Ti": 'title',
                         "F": 'field_of_study_ids',
                         "AA": 'mag_authors',
                         "CC": 'citation_count'}

        title_id_lookup = defaultdict(list)

        # for batch in batched_titles
        for count, expr in enumerate(build_expr(batched_titles(arxiv_ids_to_process,
                                                               title_id_lookup,
                                                               10000,
                                                               self.engine), 'Ti'), 1):
            logging.debug(expr)
            expr_len = len(expr.split(','))
            logging.info(f"Querying {expr_len} titles")

            data = query_mag_api(expr, paper_fields, mag_subscription_key)

            entities_len = len(data['entities'])
            logging.info(f"{entities_len} entities returned")
            missing_articles = expr_len - entities_len
            if missing_articles != 0:
                logging.info(f"{missing_articles} titles not found in MAG")

            batch_missing_fos = set()
            batch_article_fos_links = []

            # renaming and reformatting
            for row in data['entities']:
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

                # TODO:work on this to determine the best method of generating the link
                # table and calculating the missing fids
                row['field_of_study_ids'] = [f['FId'] for f in row['field_of_study_ids']]
                batch_article_fos_links.update([{row['id']: f['FId']}
                                                    for f in row['fields_of_study']]
                # ********

                if row.get('citation_count', None) is not None:
                    row['citation_count_updated'] = date.today()

                row['id'] = title_id_lookup[row['title']]

                missing_fos = set(row['field_of_study_ids']) - all_fos_ids
                if missing_fos:
                    logging.warning(f"missing field of study ids {missing_fos} for {row['id']}")
                    batch_missing_fos.update(missing_fos)


                

            # query the missing from the api and append them to the db
                # not possible to write to the link table without them present due
                # to the foreign key
            # append additional data to the arxiv table(authors, citation,
                # citation_date)
            # create entries in the fos link table (last action due to foreign key and
                # the fact that this signifies success

            logging.info(f"Batch {count}")
            if count > 1:
                logging.warning("Exiting after 1 batch in test mode")
                break

        # mark as done
        logging.warning("Task complete")
        self.output().touch()
