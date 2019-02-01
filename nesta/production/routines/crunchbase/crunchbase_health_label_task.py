"""
Crunchbase organisation health labeling
=======================================

Luigi routine to determine if crunchbase orgs are involved in health and apply a label
to the data in MYSQL.
"""

import boto3
import luigi
import logging
import os

from crunchbase_geocode_task import OrgGeocodeTask
from nesta.packages.crunchbase.crunchbase_collect import predict_health_flag
from nesta.production.luigihacks.misctools import get_config, find_filepath_from_pathstub
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.orms.crunchbase_orm import Base, Organization, OrganizationCategory
from nesta.production.orms.orm_utils import get_mysql_engine, try_until_allowed, db_session


class HealthLabelTask(luigi.Task):
    """Apply health labels to the organisation data in MYSQL.

    Args:
        _routine_id (str): String used to label the AWS task
        db_config_path: (str) The output database configuration
    """
    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    test = luigi.BoolParameter()
    insert_batch_size = luigi.IntParameter(default=500)
    db_config_env = luigi.Parameter()
    bucket = luigi.Parameter()
    vectoriser_key = luigi.Parameter()
    classifier_key = luigi.Parameter()

    def requires(self):
        yield OrgGeocodeTask(date=self.date,
                             _routine_id=self._routine_id,
                             test=self.test,
                             db_config_env="MYSQLDB",
                             city_col=Organization.city,
                             country_col=Organization.country,
                             location_key_col=Organization.location_id,
                             insert_batch_size=self.insert_batch_size,
                             env_files=[find_filepath_from_pathstub("nesta/nesta/"),
                                        find_filepath_from_pathstub("config/mysqldb.config")],
                             job_def="py36_amzn1_image",
                             job_name=f"CrunchBaseOrgGeocodeTask-{self._routine_id}",
                             job_queue="HighPriority",
                             region_name="eu-west-2",
                             poll_time=10,
                             memory=4096,
                             max_live_jobs=2)

    def output(self):
        """Points to the output database engine"""
        self.db_config_path = os.environ[self.db_config_env]
        db_config = get_config(self.db_config_path, "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Crunchbase <dummy>"  # Note, not a real table
        update_id = "CrunchbaseHealthLabel_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        """Apply health labels using model."""

        # database setup
        database = 'dev' if self.test else 'production'
        logging.warning(f"Using {database} database")
        self.engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)
        try_until_allowed(Base.metadata.create_all, self.engine)

        # collect picked models from s3
        logging.info("Collecting models from S3")
        s3 = boto3.resource('s3')
        vectoriser_obj = s3.Object(self.bucket, self.vectoriser_key)
        vectoriser = vectoriser_obj.get()['Body']._raw_stream.read()
        classifier_obj = s3.Object(self.bucket, self.classifier_key)
        classifier = classifier_obj.get()['Body']._raw_stream.read()

        # retrieve organisations and categories
        nrows = 1000 if self.test else None
        logging.info("Collecting organisations from database")
        orgs_with_cats = []
        with db_session(self.engine) as session:
            orgs = session.query(Organization.id).limit(nrows).all()
            for (org_id, ) in orgs:
                categories = (session
                              .query(OrganizationCategory.category_name)
                              .filter(OrganizationCategory.organization_id == org_id)
                              .all())
                categories = ','.join(cat_name for (cat_name, ) in categories)
                orgs_with_cats.append(dict(id=org_id, categories=categories))
        logging.info(f"{len(orgs_with_cats)} organisations retrieved from database")

        logging.info("Predicting health flags")
        orgs_with_flag = predict_health_flag(orgs_with_cats, vectoriser, classifier)

        logging.info(f"{len(orgs_with_flag)} organisations to update")
        with db_session(self.engine) as session:
            session.bulk_update_mapping(Organization, orgs_with_flag)
        #     for count, org in orgs_with_flag:
        #         session.query(Organization.id).filter(Organization.id == org['id']).update(org)

        # mark as done
        logging.warning("Task complete")
        self.output().touch()