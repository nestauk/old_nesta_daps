"""
Crunchbase data collection and processing
==================================

Luigi routine to collect Crunchbase data exports and load the data into MySQL.
"""

import luigi
import logging
import os

from nesta.packages.crunchbase.crunchbase_collect import get_files_from_tar, process_orgs, rename_uuid_columns
from nesta.packages.crunchbase.crunchbase_collect import total_records, split_batches
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
    database = 'dev' if test else 'production'

    def _insert_data(self, table, data, batch_size=1000):
        """Writes out a dataframe to MySQL and checks totals are equal, or raises error.

        Args:
            table (:obj:`sqlalchemy.mapping`): table where the data should be written
            data (:obj:`list` of :obj:`dict`): data to be written
            batch_size (int): size of bulk inserts into the db
        """
        total_rows_in = len(data)
        logging.info(f"Inserting {total_rows_in} rows of data into {table.__tablename__}")

        totals = None
        for batch in split_batches(data, batch_size):
            returned = {}
            returned['inserted'], returned['existing'], returned['failed'] = insert_data(
                                                            self.db_config_env, 'mysqldb',
                                                            self.database,
                                                            Base, table, batch,
                                                            return_non_inserted=True)
            totals = total_records(returned, totals)
            for k, v in totals.items():
                logging.info(f"{k} rows: {v}")
            logging.info("--------------")
            if totals['batch_total'] != len(batch):
                raise ValueError(f"Inserted {table} data is not equal to original: {len(batch)}")

        if totals['total'] != total_rows_in:
            raise ValueError(f"Inserted {table} data is not equal to original: {total_rows_in}")

    def output(self):
        """Points to the output database engine"""
        self.db_config_path = os.environ[self.db_config_env]
        db_config = get_config(self.db_config_path, "mysqldb")
        db_config["database"] = self.database
        db_config["table"] = "Crunchbase <dummy>"  # Note, not a real table
        update_id = "CrunchbaseCollectOrgData_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        """Collect and process organizations, categories and long descriptions."""

        # database setup
        logging.warning(f"Using {self.database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', self.database)
        try_until_allowed(Base.metadata.create_all, self.engine)

        # collect files
        cat_groups, orgs, org_descriptions = get_files_from_tar(['category_groups',
                                                                 'organizations',
                                                                 'organization_descriptions'
                                                                 ],
                                                                test=self.test)
        # process category_groups
        cat_groups = rename_uuid_columns(cat_groups)
        self._insert_data(CategoryGroup, cat_groups.to_dict(orient='records'))

        # process organizations and categories
        processed_orgs, org_cats, missing_cat_groups = process_orgs(orgs, cat_groups, org_descriptions)
        self._insert_data(CategoryGroup, missing_cat_groups)
        self._insert_data(Organization, processed_orgs, self.insert_batch_size)

        # link table needs to be inserted via non-bulk method to enforce relationship
        org_cats = [OrganizationCategory(**org_cat) for org_cat in org_cats]
        with db_session(self.engine) as session:
            session.add_all(org_cats)

        # mark as done
        self.output().touch()
