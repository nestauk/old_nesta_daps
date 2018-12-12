"""
Crunchbase data collection and processing
==================================

Luigi routine to collect Crunchbase data exports and load the data into MySQL.
"""

import luigi
import logging
import os

from nesta.packages.crunchbase.crunchbase_collect import get_files_from_tar, process_orgs, rename_uuid_columns
from nesta.production.luigihacks.misctools import get_config
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.orms.crunchbase_orm import Base, CategoryGroup, Organization, OrganizationCategory
from nesta.production.orms.orm_utils import get_mysql_engine, try_until_allowed, insert_data


class OrgCollectTask(luigi.Task):
    """Download tar file of csvs and load them into the MySQL server.

    Args:
        _routine_id (str): String used to label the AWS task
        db_config_path: (str) The output database configuration
    """
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    db_config_env = luigi.Parameter()
    test = luigi.Parameter(default=True)
    database = 'production' if not test else 'dev'

    @staticmethod
    def _total_records(data_dict):
        """Calculates totals for a dictionary of records and appends a grand total."""
        totals = {}
        total = 0
        for k, v in data_dict.items():
            length = len(v)
            totals[k] = length
            total += length
        totals['total'] = total
        return totals

    def _insert_data(self, table, data):
        """Writes out a dataframe to MySQL and checks totals are equal, or raises error.

        Args:
            table (:obj:`sqlalchemy.mapping`): table where the data should be written
            data (:obj:`pandas.DataFrame`): data to be written
        """
        rows = data.to_dict(orient='records')
        returned = {}
        returned['inserted'], returned['existing'], returned['failed'] = insert_data(
                                                        self.db_config_path, 'mysqldb',
                                                        self.database,
                                                        Base, table, rows,
                                                        return_non_inserted=True)
        totals = self._total_records(returned)
        for k, v in totals:
            logging.warning(f"{k} rows: {v}")
        if totals['total'] != len(data):
            raise ValueError(f"Inserted {table} data is not equal to original: {len(data)}")

    def output(self):
        """Points to the output database engine"""
        self.db_config_path = os.environ[self.db_config_env]
        db_config = get_config(self.db_config_path, "mysqldb")
        db_config["database"] = self.database
        db_config["table"] = "Crunchbase <dummy>"  # Note, not a real table
        update_id = "CrunchbaseCollectOrgData_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        """Collect organizations and associated tables and process them."""

        # database setup
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', self.database)
        try_until_allowed(Base.metadata.create_all, self.engine)

        # collect files
        cat_groups, orgs, org_descriptions = get_files_from_tar(['category_groups',
                                                                 'organizations',
                                                                 'organization_descriptions'])
        # process category_groups
        cat_groups = rename_uuid_columns(cat_groups)
        self._insert_data(CategoryGroup, cat_groups)

        # process organizations and categories
        processed_orgs, org_cats = process_orgs(orgs, cat_groups, org_descriptions)
        self.insert_data(Organization, processed_orgs)
        self.insert_data(OrganizationCategory, org_cats)

        # mark as done
        self.output.touch()
