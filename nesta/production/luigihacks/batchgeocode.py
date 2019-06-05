import boto3
import json
import logging
import luigi
from sqlalchemy import collate
import time
import os

from nesta.packages.geo_utils.geocode import generate_composite_key
from nesta.packages.geo_utils.country_iso_code import country_iso_code_to_name
from nesta.production.luigihacks.autobatch import AutoBatchTask
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub
from nesta.production.orms.geographic_orm import Base, Geographic
from nesta.production.orms.orm_utils import get_mysql_engine, try_until_allowed, insert_data, db_session
from nesta.production.luigihacks.misctools import get_config
from nesta.production.luigihacks.mysqldb import MySqlTarget

class GeocodeBatchTask(AutoBatchTask):
    """Appends various geographic codes to the geographic_data table using the
    `city` and `country` from the input table: lat/long, iso codes, continent.

    To implement this task, only the `output` and `combine` methods need to be defined
    when it is subclassed.

    Args:
        test (bool): in test or production mode
        db_config_env (str): environmental variable pointing to the db config file
        city_col (:obj:`sqlalchemy.Column`): column containing the city
        country_col (:obj:`sqlalchemy.Column`): column containing the full name of the country
        location_key_col (:obj:`sqlalchemy.Column`): column containing the generated composite key
        batch_size (int): number of locations to geocode in a batch
        intermediate_bucket (str): s3 bucket where the batch data will be stored
        batchable (str): location of the batchable run.py
    """
    test = luigi.BoolParameter()
    _routine_id = luigi.Parameter(default="DUMMY ROUTINE")
    db_config_env = luigi.Parameter()
    city_col = luigi.Parameter()
    country_col = luigi.Parameter()
    country_is_iso2 = luigi.BoolParameter(default=False)
    location_key_col = luigi.Parameter(default=None)
    batch_size = luigi.IntParameter(default=1000)
    intermediate_bucket = luigi.Parameter(default="nesta-production-intermediate")
    batchable = luigi.Parameter(default=find_filepath_from_pathstub("batchables/batchgeocode"))

    def output(self):
        '''Points to the output database engine'''
        db_config = get_config(os.environ[self.db_config_env], "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = f"BatchGeocode{self._routine_id} <dummy>"  # Note, not a real table
        return MySqlTarget(update_id=f"BatchGeocode-{self._routine_id}", **db_config)


    def _insert_new_locations(self):
        """Checks for new city/country combinations and appends them to the geographic
        data table in mysql.
        """
        limit = 100 if self.test else None
        with db_session(self.engine) as session:
            existing_location_ids = {i[0] for i in session.query(Geographic.id).all()}
            new_locations = []
            for city, country, key in (session.query(
                                            self.city_col,
                                            self.country_col,
                                            self.location_key_col)
                                       .distinct(self.location_key_col)
                                       .limit(limit)):
                if key not in existing_location_ids and key is not None:
                    logging.info(f"new location {city}, {country}")
                    new_locations.append(dict(id=key, city=city, country=country))
                    existing_location_ids.add(key)

        if new_locations:
            logging.warning(f"Adding {len(new_locations)} new locations to database")
            insert_data(self.db_config_env, "mysqldb", self.database,
                        Base, Geographic, new_locations)

    def _insert_new_locations_no_id(self):
        """Checks for new city/country combinations and appends them to the geographic
        data table in mysql IF NO location_key_col IS PROVIDED.
        """
        limit = 100 if self.test else None        
        with db_session(self.engine) as session:
            existing_location_ids = {i[0] for i in session.query(Geographic.id).all()}
            new_locations = []
            all_locations = {(city, country) for city, country in
                             (session.query(self.city_col, self.country_col).limit(limit))}
            nulls = 0
            for city, country in all_locations:
                if self.country_is_iso2:
                    country = country_iso_code_to_name(country, iso2=True)
                if city is None or country is None:
                    nulls += 1
                    continue
                key = generate_composite_key(city, country)
                if key not in existing_location_ids and key is not None:
                    logging.info(f"new location {city}, {country}")
                    new_locations.append(dict(id=key, city=city, country=country))
                    existing_location_ids.add(key)

        if nulls > 0:
            logging.warning(f"{nulls} locations had a null city or "
                            "country, so won't be processed.")
        if new_locations:
            logging.warning(f"Adding {len(new_locations)} new locations to database")
            insert_data(self.db_config_env, "mysqldb", self.database,
                        Base, Geographic, new_locations)


    def _get_uncoded(self):
        """Identifies all the locations in the geographic data table which have not
        previously been processed. If there are none to encode an empty list is
        returned.

        Returns:
            (:obj:`list` of :obj:`dict`) records to process
        """
        with db_session(self.engine) as session:
            uncoded = session.query(Geographic.id, Geographic.city, Geographic.country).filter(Geographic.done == False)
            uncoded = [u._asdict() for u in uncoded]
        logging.info(f"{len(uncoded)} locations to geocode")
        return uncoded

    def _create_batches(self, uncoded_locations):
        """Generate batches of records. A small batch is generated if in test mode.

        Args:
            uncoded_locations (:obj:`list` of :obj:`dict`): all locations requiring coding

        Returns:
            (str): name of each file in the s3 bucket (key)
        """
        batch_size = 50 if self.test else self.batch_size
        logging.info(f"batch size: {batch_size}")
        batch = []
        for location in uncoded_locations:
            batch.append(location)
            if len(batch) == batch_size:
                yield self._put_batch(batch)
                batch.clear()
        # catch any remainder
        if len(batch) > 0:
            yield self._put_batch(batch)

    def _put_batch(self, data):
        """Writes out a batch of data to s3 as json, so it can be picked up by the
        batchable task.

        Args:
            data (:obj:`list` of :obj:`dict`): a batch of records

        Returns:
            (str): name of the file in the s3 bucket (key)
        """
        timestamp = str(time.time()).replace('.', '')
        filename = ''.join(['geocoding_batch_', timestamp, '.json'])
        obj = self.s3.Object(self.intermediate_bucket, filename)
        obj.put(Body=json.dumps(data))
        return filename

    def prepare(self):
        """Copies any new city/county combinations from the input table into the
        geographic_data table. All rows which have previously not been processed will
        be split into batches.

        Returns:
            (:obj:`list` of :obj:`dict`) job parameters for each of the batch tasks
        """
        # set up database connectors
        self.database = 'dev' if self.test else 'production'
        self.engine = get_mysql_engine(self.db_config_env, "mysqldb", self.database)
        try_until_allowed(Base.metadata.create_all, self.engine)

        # s3 setup
        self.s3 = boto3.resource('s3')

        # identify new locations in the input table and copy them to the geographic table
        if self.location_key_col is not None:
            self._insert_new_locations()
        else:
            self._insert_new_locations_no_id()

        # create batches from all locations which have not previously been coded
        job_params = []
        uncoded_locations = self._get_uncoded()
        if uncoded_locations:
            for batch_file in self._create_batches(uncoded_locations):
                params = {"batch_file": batch_file,
                          "config": 'mysqldb.config',
                          "db_name": self.database,
                          "bucket": self.intermediate_bucket,
                          "done": False,
                          "outinfo": '',
                          "test": self.test}
                job_params.append(params)
                logging.info(params)
            logging.info(f"{len(job_params)} batches to run")
        else:
            logging.warning(f"no new locations to geocode")

        return job_params

    def combine(self, job_params):
        '''Touch the checkpoint'''
        self.output().touch()


if __name__ == '__main__':
    from nesta.production.orms.crunchbase_orm import Organization

    class MyTask(GeocodeBatchTask):
        pass

    geo = MyTask(job_def='', job_name='', job_queue='', region_name='',
                 city_col=Organization.city, country_col=Organization.country,
                 location_key_col=Organization.location_id, db_config_env='MYSQLDB',
                 test=False)
    geo.prepare()
