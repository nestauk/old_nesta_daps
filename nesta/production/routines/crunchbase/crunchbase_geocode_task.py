'''
Crunchbase geocoding
==================================

Luigi routines to geocode the Organization, FundingRound, Investor, Ipo and People tables.
'''

import logging
import luigi
import os

from crunchbase_non_org_collect_task import NonOrgCollectTask
from nesta.production.luigihacks.batchgeocode import GeocodeBatchTask
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub, get_config
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.orms.crunchbase_orm import FundingRound, Investor, Ipo, People


class OrgGeocodeTask(GeocodeBatchTask):

    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    insert_batch_size = luigi.IntParameter()

    def output(self):
        '''Points to the output database engine'''
        db_config = get_config(os.environ[self.db_config_env], "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Crunchbase <dummy>"  # Note, not a real table
        update_id = "CrunchbaseGeocodeOrgs_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def requires(self):
            '''Collects the database configurations and executes the central task.'''

            logging.getLogger().setLevel(logging.INFO)
            yield FundingRoundGeocodeTask(date=self.date,
                                          _routine_id=self._routine_id,
                                          test=not self.production,
                                          db_config_env="MYSQLDB",
                                          city_col=FundingRound.city,
                                          country_col=FundingRound.country,
                                          location_key_col=FundingRound.location_id,
                                          insert_batch_size=self.insert_batch_size,
                                          env_files=[find_filepath_from_pathstub("nesta/nesta/"),
                                                     find_filepath_from_pathstub("config/mysqldb.config"),
                                                     find_filepath_from_pathstub("config/crunchbase.config")],
                                          job_def="py36_amzn1_image",
                                          job_name=f"CrunchBaseFundingRoundGeocodeTask-{self._routine_id}",
                                          job_queue="HighPriority",
                                          region_name="eu-west-2",
                                          poll_time=10,
                                          memory=4096,
                                          max_live_jobs=2)

    def combine(self, job_params):
        '''Touch the checkpoint'''
        self.output().touch()


class FundingRoundGeocodeTask(GeocodeBatchTask):

    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    insert_batch_size = luigi.IntParameter()

    def output(self):
        '''Points to the output database engine'''
        db_config = get_config(os.environ[self.db_config_env], "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Crunchbase <dummy>"  # Note, not a real table
        update_id = "CrunchbaseGeocodeFundingRound_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def requires(self):
            '''Collects the database configurations and executes the central task.'''

            logging.getLogger().setLevel(logging.INFO)
            yield InvestorGeocodeTask(date=self.date,
                                      _routine_id=self._routine_id,
                                      test=not self.production,
                                      db_config_env="MYSQLDB",
                                      city_col=Investor.city,
                                      country_col=Investor.country,
                                      location_key_col=Investor.location_id,
                                      insert_batch_size=self.insert_batch_size,
                                      env_files=[find_filepath_from_pathstub("nesta/nesta/"),
                                                 find_filepath_from_pathstub("config/mysqldb.config"),
                                                 find_filepath_from_pathstub("config/crunchbase.config")],
                                      job_def="py36_amzn1_image",
                                      job_name=f"CrunchBaseInvestorGeocodeTask-{self._routine_id}",
                                      job_queue="HighPriority",
                                      region_name="eu-west-2",
                                      poll_time=10,
                                      memory=4096,
                                      max_live_jobs=2)

    def combine(self, job_params):
        '''Touch the checkpoint'''
        self.output().touch()


class InvestorGeocodeTask(GeocodeBatchTask):

    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    insert_batch_size = luigi.IntParameter()

    def output(self):
        '''Points to the output database engine'''
        db_config = get_config(os.environ[self.db_config_env], "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Crunchbase <dummy>"  # Note, not a real table
        update_id = "CrunchbaseGeocodeInvestor_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def requires(self):
            '''Collects the database configurations and executes the central task.'''

            logging.getLogger().setLevel(logging.INFO)
            yield IpoGeocodeTask(date=self.date,
                                 _routine_id=self._routine_id,
                                 test=not self.production,
                                 db_config_env="MYSQLDB",
                                 city_col=Ipo.city,
                                 country_col=Ipo.country,
                                 location_key_col=Ipo.location_id,
                                 insert_batch_size=self.insert_batch_size,
                                 env_files=[find_filepath_from_pathstub("nesta/nesta/"),
                                            find_filepath_from_pathstub("config/mysqldb.config"),
                                            find_filepath_from_pathstub("config/crunchbase.config")],
                                 job_def="py36_amzn1_image",
                                 job_name=f"CrunchBaseIpoGeocodeTask-{self._routine_id}",
                                 job_queue="HighPriority",
                                 region_name="eu-west-2",
                                 poll_time=10,
                                 memory=4096,
                                 max_live_jobs=2)

    def combine(self, job_params):
        '''Touch the checkpoint'''
        self.output().touch()


class IpoGeocodeTask(GeocodeBatchTask):

    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    insert_batch_size = luigi.IntParameter()

    def output(self):
        '''Points to the output database engine'''
        db_config = get_config(os.environ[self.db_config_env], "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Crunchbase <dummy>"  # Note, not a real table
        update_id = "CrunchbaseGeocodeIpo_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def requires(self):
            '''Collects the database configurations and executes the central task.'''

            logging.getLogger().setLevel(logging.INFO)
            yield PeopleGeocodeTask(date=self.date,
                                    _routine_id=self._routine_id,
                                    test=not self.production,
                                    db_config_env="MYSQLDB",
                                    city_col=People.city,
                                    country_col=People.country,
                                    location_key_col=People.location_id,
                                    insert_batch_size=self.insert_batch_size,
                                    env_files=[find_filepath_from_pathstub("nesta/nesta/"),
                                               find_filepath_from_pathstub("config/mysqldb.config"),
                                               find_filepath_from_pathstub("config/crunchbase.config")],
                                    job_def="py36_amzn1_image",
                                    job_name=f"CrunchBasePeopleGeocodeTask-{self._routine_id}",
                                    job_queue="HighPriority",
                                    region_name="eu-west-2",
                                    poll_time=10,
                                    memory=4096,
                                    max_live_jobs=2)

    def combine(self, job_params):
        '''Touch the checkpoint'''
        self.output().touch()


class PeopleGeocodeTask(GeocodeBatchTask):

    date = luigi.DateParameter()
    _routine_id = luigi.Parameter()
    insert_batch_size = luigi.IntParameter()

    def output(self):
        '''Points to the output database engine'''
        db_config = get_config(os.environ[self.db_config_env], "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Crunchbase <dummy>"  # Note, not a real table
        update_id = "CrunchbaseGeocodePeople_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def requires(self):
            '''Collects the database configurations and executes the central task.'''

            logging.getLogger().setLevel(logging.INFO)
            yield NonOrgCollectTask(date=self.date,
                                    _routine_id=self._routine_id,
                                    test=not self.production,
                                    db_config_path=find_filepath_from_pathstub("mysqldb.config"),
                                    insert_batch_size=self.insert_batch_size,
                                    batchable=find_filepath_from_pathstub("batchables/crunchbase/crunchbase_collect"),
                                    env_files=[find_filepath_from_pathstub("nesta/nesta/"),
                                               find_filepath_from_pathstub("config/mysqldb.config"),
                                               find_filepath_from_pathstub("config/crunchbase.config")],
                                    job_def="py36_amzn1_image",
                                    job_name=f"CrunchBaseNonOrgCollectTask-{self._routine_id}",
                                    job_queue="HighPriority",
                                    region_name="eu-west-2",
                                    poll_time=10,
                                    memory=4096,
                                    max_live_jobs=20)

    def combine(self, job_params):
        '''Touch the checkpoint'''
        self.output().touch()
