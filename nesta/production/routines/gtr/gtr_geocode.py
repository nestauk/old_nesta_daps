"""
Geocode GTR data
================

Apply geocoding to the collected GtR data.
Add country name, iso codes and continent.
"""
import logging
import luigi
import os

from nesta.packages.geo_utils.geocode import geocode
from nesta.packages.geo_utils.country_iso_code import country_iso_code
from nesta.packages.geo_utils.alpha2_to_continent import alpha2_to_continent_mapping
from nesta.packages.gtr.get_gtr_data import get_orgs_to_geocode
from nesta.production.luigihacks.misctools import get_config
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.orms.orm_utils import get_mysql_engine, try_until_allowed, db_session
from nesta.production.orms.gtr_orm import Base, Organisation, OrganisationLocation
"""
- Identify all rows in the organisations table that have not been processed (id in the
  organisations locations table)

- Geocode any that have a postcode and are not region='Outside UK'

- Populate country_name from the address or to 'United Kingdom' if geocoding attempted

- Apply country_iso_code and continent mapping

- Append this data to the organisations locations table
"""


class GtrGeocode(luigi.Task):
    """Perform geocoding on the collected GtR organisations data

    Args:
        _routine_id (str): String used to label the AWS task
        db_config_path: (str) The output database configuration
    """
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    test = luigi.BoolParameter()
    # insert_batch_size = luigi.IntParameter(default=500)
    db_config_env = luigi.Parameter()

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

        with db_session(self.engine) as session:
            all_orgs = session.query(OrganisationLocation.id, Organisation.Addresses).limit(limit).all()
            existing_org_location_ids = session.query(OrganisationLocation.id).limit(limit).all()
        logging.info(f"{len(all_orgs)} organisations retrieved from database")
        logging.info(f"{len(existing_org_location_ids)} organisations have previously been processed")

        # convert to a list of dictionaries with the nested addresses unpacked
        orgs = get_orgs_to_geocode(all_orgs, existing_org_location_ids)
        logging.info(f"{len(orgs)} new organisations to geocode")








        # # collect files
        # nrows = 1000 if self.test else None
        # cat_groups, orgs, org_descriptions = get_files_from_tar(['category_groups',
        #                                                          'organizations',
        #                                                          'organization_descriptions'
        #                                                          ],
        #                                                         nrows=nrows)
        # # process category_groups
        # cat_groups = rename_uuid_columns(cat_groups)
        # _insert_data(self.db_config_env, 'mysqldb', database,
        #              Base, CategoryGroup, cat_groups.to_dict(orient='records'))

        # # process organizations and categories
        # with db_session(self.engine) as session:
        #     existing_orgs = session.query(Organization.id).all()
        # existing_orgs = {o[0] for o in existing_orgs}

        # processed_orgs, org_cats, missing_cat_groups = process_orgs(orgs,
        #                                                             existing_orgs,
        #                                                             cat_groups,
        #                                                             org_descriptions)
        # _insert_data(self.db_config_env, 'mysqldb', database,
        #              Base, CategoryGroup, missing_cat_groups)
        # _insert_data(self.db_config_env, 'mysqldb', database,
        #              Base, Organization, processed_orgs, self.insert_batch_size)

        # # link table needs to be inserted via non-bulk method to enforce relationship
        # org_cats = [OrganizationCategory(**org_cat) for org_cat in org_cats]
        # with db_session(self.engine) as session:
        #     session.add_all(org_cats)

        # mark as done
        self.output().touch()
