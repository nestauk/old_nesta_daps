"""
Get organisations
=================

Luigi routine to collect organisations from Crunchbase data exports and load the data into MySQL.
"""

import luigi
import logging
import os

from nesta.packages.crunchbase.crunchbase_collect import get_files_from_tar, process_orgs
from nesta.packages.crunchbase.crunchbase_collect import rename_uuid_columns
from nesta.core.luigihacks.misctools import get_config
from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.orms.crunchbase_orm import Base, CategoryGroup, Organization, OrganizationCategory
from nesta.core.orms.orm_utils import get_mysql_engine, try_until_allowed, db_session
from nesta.core.orms.orm_utils import filter_out_duplicates
from nesta.core.orms.orm_utils import insert_data
from nesta.packages.misc_utils.batches import split_batches

class OrgCollectTask(luigi.Task):
    """Download tar file of Organization csvs and load them into the MySQL server.

    Args:
        _routine_id (str): String used to label the AWS task
        db_config_path: (str) The output database configuration
    """
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    test = luigi.BoolParameter()
    insert_batch_size = luigi.IntParameter(default=200)
    db_config_env = luigi.Parameter()

    def output(self):
        """Points to the output database engine"""
        self.db_config_path = os.environ[self.db_config_env]
        db_config = get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Crunchbase <dummy>"  # Note, not a real table
        update_id = "CrunchbaseCollectOrgData_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        """Collect and process organizations, categories and long descriptions."""

        # database setup
        database = 'dev' if self.test else 'production'
        logging.warning(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)
        try_until_allowed(Base.metadata.create_all, self.engine)

        # collect files
        nrows = 200 if self.test else None
        cat_groups, orgs, org_descriptions = get_files_from_tar(['category_groups',
                                                                 'organizations',
                                                                 'organization_descriptions'
                                                                 ],
                                                                nrows=nrows)
        # process category_groups
        cat_groups = rename_uuid_columns(cat_groups)
        insert_data(self.db_config_env, 'mysqldb', database,
                    Base, CategoryGroup, 
                    cat_groups[['id', 'name', 'category_groups_list']].to_dict(orient='records'), 
                    low_memory=True)

        # process organizations and categories
        with db_session(self.engine) as session:
            existing_orgs = session.query(Organization.id).all()
        existing_orgs = {org[0] for org in existing_orgs}

        logging.info("Summary of organisation data:")
        logging.info(f"Total number of organisations:\t {len(orgs)}")
        logging.info(f"Number of organisations already in the database:\t {len(existing_orgs)}")
        logging.info(f"Number of category groups and text descriptions:\t"
                     f"{len(cat_groups)}, {len(org_descriptions)}")

        processed_orgs, org_cats, missing_cat_groups = process_orgs(orgs,
                                                                    existing_orgs,
                                                                    cat_groups,
                                                                    org_descriptions)
        # Insert CatGroups
        insert_data(self.db_config_env, 'mysqldb', database,
                    Base, CategoryGroup, missing_cat_groups)
        # Insert orgs in batches
        n_batches = round(len(processed_orgs)/self.insert_batch_size)
        logging.info(f"Inserting {n_batches} batches of size {self.insert_batch_size}")
        for i, batch in enumerate(split_batches(processed_orgs, self.insert_batch_size)):
            if i % 100 == 0:
                logging.info(f"Inserting batch {i} of {n_batches}")
            insert_data(self.db_config_env, 'mysqldb', database,
                        Base, Organization, batch, low_memory=True)

        # link table needs to be inserted via non-bulk method to enforce relationship
        logging.info("Filtering duplicates...")
        org_cats, existing_org_cats, failed_org_cats = filter_out_duplicates(self.db_config_env, 
                                                                             'mysqldb', database, Base, 
                                                                             OrganizationCategory, 
                                                                             org_cats, 
                                                                             low_memory=True)
        logging.info(f"Inserting {len(org_cats)} org categories "
                     f"({len(existing_org_cats)} already existed and {len(failed_org_cats)} failed)")
        #org_cats = [OrganizationCategory(**org_cat) for org_cat in org_cats]
        with db_session(self.engine) as session:
            session.add_all(org_cats)

        # mark as done
        self.output().touch()
