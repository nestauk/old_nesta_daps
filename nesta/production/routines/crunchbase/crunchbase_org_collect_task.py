"""
Crunchbase data collection and processing
==================================

Luigi routine to collect Crunchbase data exports and load the data into MySQL.
"""

import luigi
import logging
import os

from nesta.packages.crunchbase.crunchbase_collect import get_files_from_tar, process_orgs
from nesta.packages.crunchbase.crunchbase_collect import _insert_data, rename_uuid_columns
from nesta.production.luigihacks.misctools import get_config
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.orms.crunchbase_orm import Base, CategoryGroup, Organization, OrganizationCategory
from nesta.production.orms.orm_utils import get_mysql_engine, try_until_allowed, insert_data, db_session


class OrgCollectTask(luigi.Task):
    """Download tar file of Organization csvs and load them into the MySQL server.

    Args:
        _routine_id (str): String used to label the AWS task
        db_config_path: (str) The output database configuration
    """
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    db_config_env = luigi.Parameter()
    insert_batch_size = luigi.IntParameter(default=1000)
    test = luigi.BoolParameter()

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
        cat_groups, orgs, org_descriptions = get_files_from_tar(['category_groups',
                                                                 'organizations',
                                                                 'organization_descriptions'
                                                                 ],
                                                                test=self.test)
        # process category_groups
        cat_groups = rename_uuid_columns(cat_groups)
        _insert_data(self.db_config_env, 'mysqldb', database,
                     Base, CategoryGroup, cat_groups.to_dict(orient='records'))

        # process organizations and categories
        with db_session(self.engine) as session:
            existing_orgs = session.query(Organization.id).all()
        existing_orgs = {o[0] for o in existing_orgs}

        processed_orgs, org_cats, missing_cat_groups = process_orgs(orgs,
                                                                    existing_orgs,
                                                                    cat_groups,
                                                                    org_descriptions)
        _insert_data(self.db_config_env, 'mysqldb', database,
                     Base, CategoryGroup, missing_cat_groups)
        _insert_data(self.db_config_env, 'mysqldb', database,
                     Base, Organization, processed_orgs, self.insert_batch_size)

        # link table needs to be inserted via non-bulk method to enforce relationship
        org_cats = [OrganizationCategory(**org_cat) for org_cat in org_cats]
        with db_session(self.engine) as session:
            session.add_all(org_cats)

        # mark as done
        self.output().touch()
