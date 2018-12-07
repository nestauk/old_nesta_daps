'''
Crunchbase data collection and processing
==================================

Luigi routine to collect Crunchbase data exports and load the data into MySQL.
'''

import luigi
import logging

from nesta.packages.crunchbase.crunchbase_collect import crunchbase_tar, get_csvs
from nesta.packages.crunchbase.crunchbase_collect import get_files_from_tar, rename_uuid_columns
from nesta.packages.crunchbase.crunchbase_collect import process_orgs
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub, get_config
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.orms.crunchbase_orm import Base, CategoryGroup, Organization, OrganizationCategory
from nesta.production.orms.orm_utils import get_mysql_engine, try_until_allowed, insert_data

import boto3

S3 = boto3.resource('s3')
_BUCKET = S3.Bucket("nesta-production-intermediate")
DONE_KEYS = set(obj.key for obj in _BUCKET.objects.all())


class OrgCollectTask(luigi.Task):
    '''Download tar file of csvs and load them into the MySQL server.

    Args:
        _routine_id (str): String used to label the AWS task
        db_config_path: (str) The output database configuration
    '''
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    db_config_path = luigi.Parameter()
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
        '''Points to the output database engine'''
        db_config = get_config(self.db_config_path, "mysqldb")
        db_config["database"] = self.database
        db_config["table"] = "Crunchbase <dummy>"  # Note, not a real table
        update_id = "CrunchbaseCollectOrgData_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        '''Collect the organizations table and associated tables and process them.
        - First process category_groups
        - Organizations is next:
            - Populate organization_category link table in place of the category column
            - Convert country code into country name and rename column
            - Composite key for the location generated and added as a new column
        - Org_parents flattened and added as a new column in Organizations
        - Organization_descriptions flattened and added as a new column in Organizations
        '''

        # database setup
        self.engine = get_mysql_engine(self.db_config_path, 'mysqldb', self.database)
        try_until_allowed(Base.metadata.create_all, self.engine)

        # collect files
        cat_groups, orgs, org_descriptions = get_files_from_tar(['category_groups',
                                                                 'organizations',
                                                                 'organization_descriptions'])
        # process category_groups
        cat_groups = rename_uuid_columns(cat_groups)
        self._insert_data(cat_groups)

        # process organizations
        processed_orgs, org_cats = process_orgs(orgs, cat_groups, org_descriptions)

        job_params = []
        for csv in get_csvs():
            logging.info("Extracting table {}...".format(csv))
            table_name = ''.join(["crunchbase_{}", csv])
            done = table_name in DONE_KEYS
            params = {"table_name": table_name,
                      "config": "mysqldb.config",
                      "db_name": "production" if not self.test else "dev",
                      "done": done}
            job_params.append(params)
        return job_params
