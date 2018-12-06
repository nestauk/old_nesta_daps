import boto3
import json
import logging
import luigi
from sqlalchemy.orm.exc import NoResultFound
import time

from nesta.production.luigihacks.autobatch import AutoBatchTask
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub
from nesta.production.orms.geographic_orm import Base, Geographic
from nesta.production.orms.orm_utils import get_mysql_engine, try_until_allowed, insert_data, db_session


class GeocodeBatchTask(AutoBatchTask):
    """Appends various geographic codes to the geographic_data table using the
    `city` and `country` from the input table: lat/long, iso codes, continent.

    To implement this task, only the `output` and `combine` methods need to be defined
    when it is subclassed.

    Args:
        city_col (:obj:`sqlalchemy.Column`): column containing the city
        country_col (:obj:`sqlalchemy.Column`): column containing the full name of the country
        composite_key_col (:obj:`sqlalchemy.Column`): column containing the generated composite key
        database_config (str): environmental variable pointing to the db config file
        database (str): name of the database, ie dev or production
        batch_size (int): number of locations to geocode in a batch
        intermediate_bucket (str): s3 bucket where the batch data will be stored
        batchable (str): location of the batchable run.py
    """
    city_col = luigi.Parameter()
    country_col = luigi.Paramater()
    composite_key_col = luigi.Parameter()
    database_config = luigi.Parameter()
    database = luigi.Parameter()
    batch_size = luigi.Parameter(default=1000)
    intermediate_bucket = luigi.Paramater(default="nesta-production-intermediate")
    batchable = luigi.paramater(default=find_filepath_from_pathstub("batchables/batchgeocode"))

    def _insert_new_locations(self):
        """Checks for new city/country combinations and appends them to the geographic
        data table in mysql.
        """
        with db_session(self.engine) as session:
            new_locations = []
            for city, country, key in session.query(self.city_col,
                                                    self.country_col,
                                                    self.composite_key_col):
                try:
                    session.query(Geographic).filter(Geographic.id == key).one()
                except NoResultFound:
                    logging.info(f"new location {city}, {country}")
                    new_locations.append(dict(id=key, city=city, country=country))

        insert_data(self.database_config, "mysqldb", self.database,
                    Base, Geographic, new_locations)

    def _get_uncoded(self):
        """Identifies all the locations in the geographic data table which have not
        previously been processed.

        Returns:
            (:obj:`list` of :obj:`Geographic`) records to process
        """
        with db_session(self.engine) as session:
            uncoded = session.query(Geographic).filter(Geographic.done == False).all()
            logging.info(f"{len(uncoded)} locations to geocode")
            return uncoded

    def _put_batch(self, data):
        """Writes out a batch of data to s3 as json, so it can be picked up by the
        batchable task.

        Args:
            data (:obj:`list` of :obj:`dict`): a batch of records

        Returns:
            (str): name of the file in the s3 bucket (key)
        """
        filename = ''.join(['geocoding_batch_', time.time(), '.json'])
        obj = self.s3.Object(self.intermediate_bucket, filename)
        obj.put(Body=json.dumps(data))
        return filename

    def _create_batches(self, uncoded_locations):
        """Generate batches of records. A small batch is generated if in test mode.

        Args:
            uncoded_locations (:obj:`list` of :obj:`Geographic`): all records,
                as returned from sqlalchemy

        Returns:
            (str): name of each file in the s3 bucket (key)
        """
        batch_size = 50 if self.test else self.batch_size
        logging.info(f"batch size: {batch_size}")
        batch = []
        for location in uncoded_locations:
            batch.append(dict(id=location.id, city=location.city, country=location.country))
            if len(batch) == batch_size:
                yield self._put_batch(batch)
                batch.clear()
        yield self._put_batch(batch)

    def prepare(self):
        """Copies any new city/county combinations from the input table into the
        geographic_data table. All rows which have previously not been processed will
        be split into batches.

        Returns:
            (:obj:`list` of :obj:`dict`) job parameters for each of the batch tasks
        """
        # set up database connectors
        self.engine = get_mysql_engine(self.database_config, "mysqldb", self.database_name)
        try_until_allowed(Base.metadata.create_all, self.engine)

        # s3 setup
        self.s3 = boto3.resource('s3')

        # identify new locations in the input table and copy them to the geographic table
        self._insert_new_locations()

        # create batches from all locations which have not previously been coded
        uncoded_locations = self._get_uncoded()
        job_params = []
        for batch_file in self._create_batches(uncoded_locations):
            params = {"batch_file": batch_file,
                      "config": self.database_config,
                      "db_name": self.database_name,
                      "bucket": self.intermediate_bucket,
                      "done": False,
                      "outinfo": ''}
            job_params.append(params)
            logging.info(params)
        logging.info(f"{len(job_params)} batches to run")
        return job_params
