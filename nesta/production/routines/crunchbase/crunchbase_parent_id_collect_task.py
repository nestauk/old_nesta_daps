'''
Crunchbase data collection and processing
==================================

Luigi routine to collect Crunchbase data exports and load the data into MySQL.

This task picks up the missed org_parents table and combines this with organizations.
'''
import boto3
import logging
import luigi

from crunchbase_health_label_task import HealthLabelTask
from nesta.packages.crunchbase.crunchbase_collect import get_files_from_tar
from nesta.packages.misc_utils.batches import split_batches
from nesta.production.luigihacks import misctools
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.orms.crunchbase_orm import Organization
from nesta.production.orms.orm_utils import get_mysql_engine, db_session


S3 = boto3.resource('s3')
_BUCKET = S3.Bucket("nesta-production-intermediate")
DONE_KEYS = set(obj.key for obj in _BUCKET.objects.all())


class ParentIdCollectTask(luigi.Task):
    '''Download tar file of csvs and append parent_ids to the organizations table.

    Args:
        date (datetime): Datetime used to label the outputs
        _routine_id (str): String used to label the AWS task
        db_config_env (str): The output database envariable
        db_config_path (str): The output database configuration
        insert_batch_size (int): number of rows to insert into the db in a batch
    '''
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    db_config_env = luigi.Parameter()
    db_config_path = luigi.Parameter()
    insert_batch_size = luigi.IntParameter(default=500)

    def requires(self):
        yield HealthLabelTask(date=self.date,
                              _routine_id=self._routine_id,
                              test=self.test,
                              insert_batch_size=self.insert_batch_size,
                              db_config_env=self.db_config_env,
                              bucket='nesta-crunchbase-models',
                              vectoriser_key='vectoriser.pickle',
                              classifier_key='clf.pickle')

    def output(self):
        '''Points to the output database engine'''
        db_config = misctools.get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Crunchbase <dummy>"  # Note, not a real table
        update_id = "CrunchbaseParentIdCollect_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        # database setup
        database = 'dev' if self.test else 'production'
        logging.warning(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)

        # collect file
        nrows = 1000 if self.test else None
        logging.info(f"Collecting {nrows if nrows else 'all'} org_parents from crunchbase tar")
        org_parents = get_files_from_tar(['org_parents'], nrows=nrows)[0]
        logging.info(f"{len(org_parents)} parent ids in crunchbase export")

        # collect previously processed orgs
        logging.info("Extracting previously processed organisations")
        with db_session(self.engine) as session:
            processed_orgs = (session.query(Organization.id)
                              .filter(Organization.parent_id.isnot(None))
                              .all())
        processed_orgs = {org for (org, ) in processed_orgs}
        logging.info(f"{len(processed_orgs)} previously processed orgs")

        # reformat into a list of dicts removing orgs that already have a parent_id
        org_parents = org_parents[['uuid', 'parent_uuid']]
        org_parents.columns = ['id', 'parent_id']
        org_parents = org_parents[~org_parents['id'].isin(processed_orgs)]
        org_parents = org_parents.to_dict(orient='records')
        logging.info(f"{len(org_parents)} organisations to update in MYSQL")

        # insert parent_ids into db in batches
        for count, batch in enumerate(split_batches(org_parents,
                                                    self.insert_batch_size), 1):
            with db_session(self.engine) as session:
                session.bulk_update_mappings(Organization, batch)
            logging.info(f"{count} batch{'es' if count > 1 else ''} written to db")

        # mark as done
        logging.warning("Task complete")
        self.output().touch()
