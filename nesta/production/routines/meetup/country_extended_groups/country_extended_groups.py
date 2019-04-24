'''
Country :math:`\rightarrow` Extended groups
===========================================

Starting with a seed country (and Meetup category),
extract all groups in that country and subsequently
find all groups associated with all members of the
original set of groups.
'''

from nesta.packages.meetup.country_groups import get_coordinate_data
from nesta.packages.meetup.country_groups import assert_iso2_key
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.luigihacks.misctools import get_config
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub
from nesta.production.luigihacks import autobatch
from nesta.production.luigihacks import s3
from nesta.production.orms.meetup_orm import Base
from nesta.production.orms.orm_utils import get_mysql_engine
import luigi
import datetime
import json
import time
import logging 
from botocore.errorfactory import ClientError
import boto3
import os


# Define these globally since they are shared resources
# TODO: consider bundling this into a Singleton
S3 = boto3.resource('s3')
_BUCKET = S3.Bucket("nesta-production-intermediate")
DONE_KEYS = set(obj.key for obj in _BUCKET.objects.all())
BATCHABLE = os.path.join(find_filepath_from_pathstub("production/batchables/meetup"),"{}")

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


class CountryGroupsTask(autobatch.AutoBatchTask):
    '''Extract all groups with corresponding category for this country.

    Args:
    
    
    '''
    iso2 = luigi.Parameter()
    category = luigi.Parameter()
    _routine_id = luigi.Parameter()

    def output(self):
        '''Points to the input database target'''
        update_id = "meetup_groups-%s" % self._routine_id
        db_config = get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "production" if not self.test else "dev"
        db_config["table"] = "meetup_groups"
        return MySqlTarget(update_id=update_id, **db_config)


    def prepare(self):
        '''Prepare the batch job parameters'''

        # Get all country data and generate the lat/lon and radius
        # parameter for this country
        df = get_coordinate_data(n=10)
        condition = assert_iso2_key(df, self.iso2)

        # Get parameters for this country
        name = df.loc[condition, "NAME"].values[0]
        coords = df.loc[condition, "coords"].values[0]
        radius = df.loc[condition, "radius"].values[0]

        # Add the mandatory `outinfo' and `done' fields
        job_params = []
        for _coords in chunks(coords, 10):
            # Check whether the job has been done already
            s3_key = self.job_name
            s3_path = "s3://nesta-production-intermediate/%s" % s3_key
            done = s3_key in DONE_KEYS
            # Fill in the params
            params = {"iso2":self.iso2,
                      "cat":self.category, "name":name,
                      "radius":radius, "coords":str(_coords),
                      "config": "mysqldb.config",
                      "db":"production" if not self.test else "dev",
                      "outinfo":s3_path, "done":done}
            job_params.append(params)
        return job_params


    def combine(self, job_params):
        '''Combine the outputs from the batch jobs'''
        self.output().touch()


class GroupsMembersTask(autobatch.AutoBatchTask):
    '''

    Args:
        date (datetime): Date used to label the outputs
        batchable (str): Path to the directory containing the run.py batchable
        job_def (str): Name of the AWS job definition
        job_name (str): Name given to this AWS batch job
        job_queue (str): AWS batch queue
        region_name (str): AWS region from which to batch
        poll_time (int): Time between querying the AWS batch job status
    '''
    iso2 = luigi.Parameter()
    category = luigi.Parameter()
    _routine_id = luigi.Parameter()

    def requires(self):
        '''Gets the input data'''
        return CountryGroupsTask(iso2=self.iso2, 
                                 category=self.category,
                                 _routine_id=self._routine_id,
                                 batchable=BATCHABLE.format("country_groups"),
                                 env_files=self.env_files,
                                 job_def=self.job_def,                                 
                                 job_name="CountryGroups-%s" % self._routine_id,
                                 job_queue=self.job_queue,
                                 region_name=self.region_name,
                                 poll_time=self.poll_time,
                                 test=self.test)


    def output(self):
        '''Points to the DB target'''
        update_id = "meetup_groups_members-%s" % self._routine_id
        db_config = get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "production" if not self.test else "dev"
        db_config["table"] = "meetup_groups_members"
        return MySqlTarget(update_id=update_id, **db_config)


    def prepare(self):
        '''Prepare the batch job parameters'''
        cnx = self.input().connect()
        cursor = cnx.cursor()
        query = ("SELECT id, urlname FROM meetup_groups "
                 "WHERE country = %s AND category_id = %s;")
        cursor.execute(query, (self.iso2, self.category))
        
        # Add the mandatory `outinfo' and `done' fields
        job_params = []
        for group_id, group_urlname in cursor:
            # Check whether the job has been done already
            s3_key = "{}-{}-{}".format(self.job_name, group_id, group_urlname)
            s3_path = "s3://nesta-production-intermediate/%s" % s3_key
            done = s3_key in DONE_KEYS
            # Fill in the params
            params = {"group_urlname":group_urlname,
                      "group_id":group_id,
                      "config":"mysqldb.config",
                      "db":"production" if not self.test else "dev",
                      "outinfo":s3_path, "done":done}
            job_params.append(params)
        # Tidy up and return
        cursor.close()
        cnx.close()
        return job_params


    def combine(self, job_params):
        '''Combine the outputs from the batch jobs'''
        self.output().touch()


class MembersGroupsTask(autobatch.AutoBatchTask):
    '''

    Args:
        date (datetime): Date used to label the outputs
        batchable (str): Path to the directory containing the run.py batchable
        job_def (str): Name of the AWS job definition
        job_name (str): Name given to this AWS batch job
        job_queue (str): AWS batch queue
        region_name (str): AWS region from which to batch
        poll_time (int): Time between querying the AWS batch job status
    '''
    iso2 = luigi.Parameter()
    category = luigi.Parameter()
    _routine_id = luigi.Parameter()

    def requires(self):
        '''Gets the input data'''
        return GroupsMembersTask(iso2=self.iso2,
                                 category=self.category,
                                 _routine_id=self._routine_id,
                                 batchable=BATCHABLE.format("groups_members"),
                                 env_files=self.env_files,
                                 job_def=self.job_def,
                                 job_name="GroupsMembers-%s" % self._routine_id,      
                                 job_queue=self.job_queue,
                                 region_name=self.region_name,
                                 poll_time=self.poll_time,
                                 test=self.test)


    def output(self):
        '''Points to the DB target'''
        update_id = "meetup_members_groups-%s" % self._routine_id
        db_config = get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "production" if not self.test else "dev"
        db_config["table"] = "meetup_members_groups"
        return MySqlTarget(update_id=update_id, **db_config)


    def prepare(self):
        '''Prepare the batch job parameters'''

        cnx = self.input().connect()
        cursor = cnx.cursor()
        query = ("SELECT member_id FROM meetup_groups_members "
                 "JOIN meetup_groups "
                 "ON meetup_groups_members.group_id = meetup_groups.id "
                 "WHERE country = %s AND category_id = %s;")
        cursor.execute(query, (self.iso2, self.category))

        job_params = []
        while True:
            chunk = cursor.fetchmany(100)
            if len(chunk) == 0:
                break
            data = [member_id for member_id, in chunk]
            # Check whether the job has been done already
            s3_key = "{}-{}-{}".format(self.job_name, data[0], data[-1])
            s3_path = "s3://nesta-production-intermediate/%s" % s3_key
            done = s3_key in DONE_KEYS
            # Fill in the params
            params = {"member_ids":str(data),
                      "config":"mysqldb.config",
                      "db":"production" if not self.test else "dev",
                      "outinfo":s3_path, "done":done}
            job_params.append(params)
        # Tidy up and return
        cursor.close()
        cnx.close()
        return job_params

        # # Add the mandatory `outinfo' and `done' fields
        # job_params = []
        # for member_id, in cursor:
        #     # Check whether the job has been done already
        #     s3_key = "{}-{}".format(self.job_name, member_id)
        #     s3_path = "s3://nesta-production-intermediate/%s" % s3_key
        #     done = s3_key in DONE_KEYS
        #     # Fill in the params
        #     params = {"member_id":member_id,
        #               "config":"mysqldb.config",
        #               "outinfo":s3_path, "done":done}
        #     job_params.append(params)
        # # Tidy up and return
        # cursor.close()
        # cnx.close()
        # return job_params


    def combine(self, job_params):
        '''Combine the outputs from the batch jobs'''
        self.output().touch()


class GroupDetailsTask(autobatch.AutoBatchTask):
    '''The root task, which adds the surname 'Muppet'
    to the names of the muppets.

    Args:
        date (datetime): Date used to label the outputs
    '''
    _routine_id = luigi.Parameter()
    iso2 = luigi.Parameter()
    category = luigi.Parameter()

    def requires(self):
        '''Get the output from the batchtask'''
        return MembersGroupsTask(iso2=self.iso2,
                                 category=self.category,
                                 _routine_id=self._routine_id,
                                 batchable=BATCHABLE.format("members_groups"),
                                 env_files=self.env_files,
                                 job_def=self.job_def,
                                 job_name="MembersGroups-%s" % self._routine_id,
                                 job_queue=self.job_queue,
                                 region_name=self.region_name,
                                 poll_time=self.poll_time,
                                 test=self.test)


    def output(self):
        '''Points to the DB target'''
        update_id = "meetup_group_details-%s" % self._routine_id
        db_config = get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "production" if not self.test else "dev"
        db_config["table"] = "meetup_groups"
        return MySqlTarget(update_id=update_id, **db_config)


    def prepare(self):
        '''Prepare the batch job parameters'''

        cnx = self.input().connect()
        cursor = cnx.cursor()
        query = ("SELECT distinct(group_urlname) "
                 "FROM meetup_groups_members "
                 "LEFT JOIN meetup_groups "
                 "ON meetup_groups_members.group_id = meetup_groups.id "
                 "WHERE meetup_groups.country IS NULL;")
        cursor.execute(query)

        # Add the mandatory `outinfo' and `done' fields
        job_params = []
        while True:
            chunk = cursor.fetchmany(100)
            if len(chunk) == 0:
                break
            data = [group_urlname for group_urlname, in chunk
                    if group_urlname.count("?") == 0]
            # Check whether the job has been done already
            s3_key = "{}-{}-{}".format(self.job_name, data[0], data[-1])
            s3_path = "s3://nesta-production-intermediate/%s" % s3_key
            done = s3_key in DONE_KEYS
            # Fill in the params
            params = {"group_urlnames":str([x.encode("utf8") 
                                            for x in data]),
                      "config":"mysqldb.config",
                      "db":"production" if not self.test else "dev",
                      "outinfo":s3_path, "done":done}
            job_params.append(params)
        # Tidy up and return
        cursor.close()
        cnx.close()
        return job_params


    def combine(self, job_params):
        '''Combine the outputs from the batch jobs'''
        self.output().touch()


class RootTask(luigi.WrapperTask):
    '''A dummy root task, which collects the database configurations
    and executes the central task. 

    Args:
        date (datetime): Date used to label the outputs
    '''
    date = luigi.DateParameter(default=datetime.date.today())
    iso2 = luigi.Parameter()
    category = luigi.Parameter()
    production = luigi.BoolParameter(default=False)
    
    def requires(self):
        '''Collects the database configurations and executes the central task.'''
        logging.getLogger().setLevel(logging.INFO)
        _routine_id = f"{self.date}-{self.iso2}-{self.category}-{self.production}"

        engine = get_mysql_engine("MYSQLDB", "mysqldb", 
                                  "production" if self.production else "dev")
        Base.metadata.create_all(engine)
        
        yield GroupDetailsTask(iso2=self.iso2,
                               category=self.category,
                               _routine_id=_routine_id,
                               batchable=BATCHABLE.format("group_details"),
                               env_files=[find_filepath_from_pathstub("/nesta/nesta"),
                                          find_filepath_from_pathstub("/config/mysqldb.config")],
                               job_def="py36_amzn1_image",
                               job_name="GroupDetails-%s" % _routine_id,
                               job_queue="HighPriority",
                               region_name="eu-west-2",
                               poll_time=10,
                               max_live_jobs=100,
                               test=(not self.production))
