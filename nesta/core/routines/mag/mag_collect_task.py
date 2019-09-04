"""
MAG data collection and processing
==================================

Luigi pipeline to collect all data from the MAG SPARQL endpoint and load it to SQL.
"""
import boto3
from botocore.exceptions import ClientError
import json
import luigi
import logging

from nesta.packages.mag.query_mag_sparql import (check_institute_exists,
                                                 get_eu_countries,
                                                 query_by_grid_id,
                                                 update_field_of_study_ids_sparql)
from nesta.packages.misc_utils.batches import split_batches
from nesta.core.orms.mag_orm import (Base, Paper, Author, PaperAuthor,
                                     PaperFieldsOfStudy, FieldOfStudy, PaperLanguage)
from nesta.core.orms.grid_orm import Institute
from nesta.core.orms.orm_utils import get_mysql_engine, db_session, insert_data
from nesta.core.luigihacks import misctools
from nesta.core.luigihacks.mysqldb import MySqlTarget


BUCKET = "nesta-production-intermediate"
INTERMEDIATE_FILE = "mag-collection-completed-institutes.json"


class MagCollectSparqlTask(luigi.Task):
    """Query MAG for Papers, and Authors who are affiliated with EU institutes.

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
    batch_size = luigi.IntParameter()
    insert_batch_size = luigi.IntParameter()
    from_date = luigi.Parameter()
    min_citations = luigi.IntParameter()

    def output(self):
        '''Points to the output database engine'''
        db_config = misctools.get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "MAG <dummy>"  # Note, not a real table
        update_id = "MagCollectSparql_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        # s3 setup
        s3 = boto3.resource('s3')
        intermediate_file = s3.Object(BUCKET, INTERMEDIATE_FILE)

        # database setup
        database = 'dev' if self.test else 'production'
        logging.info(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)
        Base.metadata.create_all(self.engine)

        eu = get_eu_countries()
        logging.info(f"Retrieved {len(eu)} EU countries")

        with db_session(self.engine) as session:
            all_fos_ids = {f.id for f in (session
                                          .query(FieldOfStudy.id)
                                          .all())}
            logging.info(f"{len(all_fos_ids):,} fields of study in database")

            eu_grid_ids = {i.id for i in (session
                                          .query(Institute.id)
                                          .filter(Institute.country.in_(eu))
                                          .all())}
            logging.info(f"{len(eu_grid_ids):,} EU institutes in GRID")

        try:
            processed_grid_ids = set(json.loads(intermediate_file
                                                .get()['Body']
                                                ._raw_stream.read()))

            logging.info(f"{len(processed_grid_ids)} previously processed institutes")
            eu_grid_ids = eu_grid_ids - processed_grid_ids
            logging.info(f"{len(eu_grid_ids):,} institutes to process")
        except ClientError:
            logging.info("Unable to load file of processed institutes, starting from scratch")
            processed_grid_ids = set()

        if self.test:
            self.batch_size = 500
            batch_limit = 1
        else:
            batch_limit = None
        testing_finished = False

        row_count = 0
        for institute_count, grid_id in enumerate(eu_grid_ids):
            paper_ids, author_ids = set(), set()
            data = {Paper: [],
                    Author: [],
                    PaperAuthor: set(),
                    PaperFieldsOfStudy: set(),
                    PaperLanguage: set()}

            if not institute_count % 50:
                logging.info(f"{institute_count:,} of {len(eu_grid_ids):,} institutes processed")

            if not check_institute_exists(grid_id):
                logging.debug(f"{grid_id} not found in MAG")
                continue

            # these tables have data stored in sets for deduping so the fieldnames will
            # need to be added when converting to a list of dicts for loading to the db
            field_names_to_add = {PaperAuthor: ('paper_id', 'author_id'),
                                  PaperFieldsOfStudy: ('paper_id', 'field_of_study_id'),
                                  PaperLanguage: ('paper_id', 'language')}

            logging.info(f"Querying MAG for {grid_id}")
            for row in query_by_grid_id(grid_id,
                                        from_date=self.from_date,
                                        min_citations=self.min_citations,
                                        batch_size=self.batch_size,
                                        batch_limit=batch_limit):

                fos_id = row['fieldOfStudyId']
                if fos_id not in all_fos_ids:
                    logging.info(f"Getting missing field of study {fos_id} from MAG")
                    update_field_of_study_ids_sparql(self.engine, fos_ids=[fos_id])
                    all_fos_ids.add(fos_id)

                # the returned data is normalised and therefore contains many duplicates
                paper_id = row['paperId']
                if paper_id not in paper_ids:
                    data[Paper].append({'id': paper_id,
                                        'title': row['paperTitle'],
                                        'citation_count': row['paperCitationCount'],
                                        'created_date': row['paperCreatedDate'],
                                        'doi': row.get('paperDoi'),
                                        'book_title': row.get('bookTitle')})
                    paper_ids.add(paper_id)

                author_id = row['authorId']
                if author_id not in author_ids:
                    data[Author].append({'id': author_id,
                                         'name': row['authorName'],
                                         'grid_id': grid_id})
                    author_ids.add(author_id)

                data[PaperAuthor].add((row['paperId'], row['authorId']))

                data[PaperFieldsOfStudy].add((row['paperId'], row['fieldOfStudyId']))

                try:
                    data[PaperLanguage].add((row['paperId'], row['paperLanguage']))
                except KeyError:
                    # language is an optional field
                    pass

                row_count += 1
                if self.test and row_count >= 1000:
                    logging.warning("Breaking after 1000 rows in test mode")
                    testing_finished = True
                    break

            # write out to SQL
            for table, rows in data.items():
                if table in field_names_to_add:
                    rows = [{k: v for k, v in zip(field_names_to_add[table], row)}
                            for row in rows]
                logging.debug(f"Writing {len(rows):,} rows to {table.__table__.name}")

                for batch in split_batches(rows, self.insert_batch_size):
                    insert_data('MYSQLDB', 'mysqldb', database, Base, table, batch)

            # flag institute as completed on S3
            processed_grid_ids.add(grid_id)
            intermediate_file.put(Body=json.dumps(list(processed_grid_ids)))

            if testing_finished:
                break


        # mark as done
        logging.info("Task complete")
        self.output().touch()
