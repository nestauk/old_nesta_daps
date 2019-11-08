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

from nesta.packages.mag.query_mag_sparql import count_papers, get_eu_countries
from nesta.core.orms.grid_orm import Institute
from nesta.core.orms.orm_utils import get_mysql_engine, db_session
from nesta.core.luigihacks import misctools
from nesta.core.luigihacks.mysqldb import MySqlTarget


BUCKET = "nesta-production-intermediate"
MAG_ENDPOINT = 'http://ma-graph.org/sparql'


class MagEstimateSparqlTask(luigi.Task):
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

    def output(self):
        '''Points to the output database engine'''
        db_config = misctools.get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "MAG <dummy>"  # Note, not a real table
        update_id = "MagCollectSparql_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        # database setup
        database = 'dev' if self.test else 'production'
        logging.info(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)

        # s3 setup
        s3 = boto3.resource('s3')
        intermediate_file = s3.Object(BUCKET, f"mag_estimate_{database}.json")

        eu = get_eu_countries()

        with db_session(self.engine) as session:
            eu_grid_ids = {i.id for i in (session
                                          .query(Institute.id)
                                          .filter(Institute.country.in_(eu))
                                          .all())}
            logging.info(f"{len(eu_grid_ids):,} EU institutes in GRID")

        # collect previous and exclude
        try:
            previous = json.loads(intermediate_file.get()['Body']._raw_stream.read())

            done_institutes = set(previous['institutes'])
            logging.info(f"{len(done_institutes)} previously processed institutes retrieved")
            eu_grid_ids = eu_grid_ids - done_institutes
            logging.info(f"{len(eu_grid_ids)} to process")

            paper_ids = set(previous['paper_ids'])
            logging.info(f"{len(paper_ids)} previously processed papers retrieved")
        except ClientError:
            logging.info("Unable to load previous file, starting from scratch")
            done_institutes = set()
            paper_ids = set()

        limit = 100 if self.test else None
        save_every = 50 if self.test else 1000000

        total = count_papers(eu_grid_ids,
                             done_institutes,
                             paper_ids,
                             intermediate_file,
                             save_every=save_every,
                             limit=limit)

        # mark as done
        logging.info("Task complete")
        logging.info(f"Total EU papers found: {total:,}")
        self.output().touch()
