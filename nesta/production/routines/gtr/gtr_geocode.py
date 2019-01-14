"""
Geocode GTR data
================

Apply geocoding to the collected GtR data.
Add country name, iso codes and continent.
"""
import logging
import luigi
from math import ceil
import os

from nesta.packages.gtr.get_gtr_data import add_country_details
from nesta.packages.gtr.get_gtr_data import geocode_uk_with_postcode
from nesta.packages.gtr.get_gtr_data import get_orgs_to_process
from nesta.packages.misc_utils.batches import split_batches
from nesta.production.luigihacks.misctools import get_config
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.orms.orm_utils import db_session, get_mysql_engine
from nesta.production.orms.orm_utils import insert_data, try_until_allowed
from nesta.production.orms.gtr_orm import Base, Organisation, OrganisationLocation
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub
from nesta.production.routines.gtr.gtr_collect import GtrTask


class GtrGeocode(luigi.Task):
    """Perform geocoding on the collected GtR organisations data

    Args:
        _routine_id (str): String used to label the AWS task
        db_config_path: (str) The output database configuration
    """
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    test = luigi.BoolParameter()
    db_config_env = luigi.Parameter()
    page_size = luigi.IntParameter()

    def requires(self):
        '''Collects the database configurations and executes the central task.'''
        logging.getLogger().setLevel(logging.INFO)
        yield GtrTask(date=self.date,
                      page_size=self.page_size,
                      batchable=find_filepath_from_pathstub("production/batchables/gtr/"),
                      env_files=[find_filepath_from_pathstub("/nesta/nesta"),
                                 find_filepath_from_pathstub("/config/mysqldb.config")],
                      job_def="py36_amzn1_image",
                      job_name=f"GtR-{self.date}-{self.page_size}-{not self.test}",
                      job_queue="HighPriority",
                      region_name="eu-west-2",
                      poll_time=10,
                      test=self.test)

    def output(self):
        """Points to the output database engine"""
        self.db_config_path = os.environ[self.db_config_env]
        db_config = get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "GtR Geocode <dummy>"  # Note, not a real table
        update_id = "GtRGeocode_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        """Collect and process organizations, categories and long descriptions."""

        # database setup
        database = 'dev' if self.test else 'production'
        logging.warning(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)
        try_until_allowed(Base.metadata.create_all, self.engine)
        limit = 2000 if self.test else None
        batch_size = 50 if self.test else 1000

        with db_session(self.engine) as session:
            all_orgs = session.query(OrganisationLocation.id, Organisation.addresses).limit(limit).all()
            existing_org_location_ids = session.query(OrganisationLocation.id).limit(limit).all()
        logging.info(f"{len(all_orgs)} organisations retrieved from database")
        logging.info(f"{len(existing_org_location_ids)} organisations have previously been processed")

        # convert to a list of dictionaries with the nested addresses unpacked
        orgs = get_orgs_to_process(all_orgs, existing_org_location_ids)
        logging.info(f"{len(orgs)} new organisations to geocode")

        total_batches = ceil(len(orgs)/batch_size)
        logging.info(f"{total_batches} batches")
        completed_batches = 0
        for batch in split_batches(orgs, batch_size=batch_size):
            map(add_country_details, batch)
            map(geocode_uk_with_postcode, batch)

            # remove data not in OrganisationLocation columns
            org_location_cols = OrganisationLocation.__table__.columns.keys()
            batch = [{k: v for k, v in org.items() if k in org_location_cols}
                     for org in orgs]

            insert_data(self.db_config_env, 'mysqldb', database,
                        Base, OrganisationLocation, batch)
            completed_batches += 1
            logging.info(f"Completed {completed_batches} of {total_batches} batches")

            if self.test:
                logging.warning("Breaking after 1 batch in test mode")
                break

        # mark as done
        logging.warning("Finished task")
        self.output().touch()
