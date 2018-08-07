'''
Country :math:`\rightarrow` Extended groups
===========================================

Starting with a seed country (and Meetup category),
extract all groups in that country and subsequently
find all groups associated with all members of the
original set of groups.
'''

from meetup.country_groups import get_coordinate_data
from meetup.country_groups import assert_iso2_key
from luigihacks.mysqldb import MySqlTarget
from luigihacks import misctools
from luigihacks import autobatch
from luigihacks import s3
import luigi
import datetime
import json
import time
import logging 
from botocore.errorfactory import ClientError
import boto3


def s3_exists(s3_path):
    s3_client = boto3.client('s3')
    bucket, key = s3.parse_s3_path(s3_path)
    try:        
        s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError:
        return False
    else:
        return True


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
    task_id = luigi.Parameter()

    def output(self):
        '''Points to the input database target'''
        update_id = "meetup_groups_"+str(self.task_id)
        db_config = misctools.get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "production"
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
            s3_path = ("s3://nesta-production-intermediate/"+
                       self.job_name)
            params = {"iso2":self.iso2, 
                      "cat":self.category, "name":name,
                      "radius":radius, "coords":str(_coords),
                      "config": "mysqldb.config",
                      "outinfo":s3_path, "done":s3_exists(s3_path)}
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
    task_id = luigi.Parameter()

    def requires(self):
        '''Gets the input data'''
        return CountryGroupsTask(iso2=self.iso2, 
                                 category=self.category,
                                 task_id=self.task_id,
                                 batchable=("/home/ec2-user/nesta/production/"
                                            "batchables/meetup/country_groups/"),
                                 env_files=self.env_files,
                                 job_def=self.job_def,                                 
                                 job_name="CountryGroups-%s" % self.task_id,
                                 job_queue=self.job_queue,
                                 region_name=self.region_name,
                                 poll_time=self.poll_time)


    def output(self):
        '''Points to the DB target'''
        update_id = "meetup_groups_members"+self.task_id
        db_config = misctools.get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "production"
        db_config["table"] = "meetup_groups_members"
        return MySqlTarget(update_id=update_id, **db_config)


    def prepare(self):
        '''Prepare the batch job parameters'''
        #### TODO: Get inputs from the DB
        cnx = self.input().connect()
        cursor = cnx.cursor()
        query = ("SELECT id, urlname FROM meetup_groups "
                 "WHERE country = %s AND category_id = %s;")
        cursor.execute(query, (self.iso2, self.category))
        cursor.close()
        cnx.close()

        # Add the mandatory `outinfo' and `done' fields
        for group_id, group_urlname in cursor:
            s3_path = ("s3://nesta-production-intermediate/"+self.job_name+
                       "-{}-{}".format(self.group_id,
                                        self.group_urlname))
            params = {"group_urlname":self.group_urlname,
                      "group_id":self.group_id,
                      "config":"mysqldb.config",
                      "outinfo":s3_path, "done":s3_exists(s3_path)}

        return job_params

    def combine(self, job_params):
        '''Combine the outputs from the batch jobs'''
        self.output().touch()


# class MembersGroupsTask(BaseGroupsMembersTask):
#     '''

#     Args:
#         date (datetime): Date used to label the outputs
#         batchable (str): Path to the directory containing the run.py batchable
#         job_def (str): Name of the AWS job definition
#         job_name (str): Name given to this AWS batch job
#         job_queue (str): AWS batch queue
#         region_name (str): AWS region from which to batch
#         poll_time (int): Time between querying the AWS batch job status
#     '''
    
#     def requires(self):
#         '''Gets the input data'''
#         return GroupsMembersTask(date=self.date, 
#                                  iso2=self.iso2, 
#                                  category=self.category,
#                                  meetup_id="group_id",
#                                  batchable="", ##### TODO: add batchable
#                                  job_def=self.job_def,
#                                  job_name="GroupsMembers-%s" % self.date,
#                                  job_queue=self.job_queue,
#                                  region_name=self.region_name,
#                                  poll_time=self.poll_time)

#     def output(self):
#         '''Points to the DB target'''
#         #### TODO: Change to DB
#         return s3.S3Target(S3PREFIX+"intermediate_output_%s.json" % self.date)


# class GroupDetailsTask(autobatch.AutoBatchTask):
#     '''The root task, which adds the surname 'Muppet'
#     to the names of the muppets.

#     Args:
#         date (datetime): Date used to label the outputs
#     '''
#     date = luigi.DateParameter()
#     iso2 = luigi.Parameter()
#     category = luigi.Parameter()

#     def requires(self):
#         '''Get the output from the batchtask'''
#         return MembersGroupsTask(date=self.date, 
#                                  iso2=self.iso2, 
#                                  category=self.category,
#                                  meetup_id="member_id",
#                                  batchable="", ##### TODO: add batchable
#                                  job_def=self.job_def,
#                                  job_name="MembersGroups-%s" % self.date,
#                                  job_queue=self.job_queue,
#                                  region_name=self.region_name,
#                                  poll_time=self.poll_time)

#     def output(self):
#         '''Points to the DB target'''
#         #### TODO: Change to DB
#         return s3.S3Target(S3PREFIX+"intermediate_output_%s.json" % self.date)

#     def prepare(self):
#         '''Prepare the batch job parameters'''
#         #### TODO: Get inputs from the DB
#         # Add the mandatory `outinfo' and `done' fields
#         for i, params in enumerate(job_params):
#             params["outinfo"] = ("s3://nesta-production-intermediate/"+
#                                  self.job_name+
#                                  "-{}-{}-{}-{}".format(self.date, 
#                                                        self.iso2,
#                                                        self.category,
#                                                        params[self.meetup_id]))
#             params["done"] = s3fs.exists(params["outinfo"])
#         return job_params

#     def combine(self, job_params):
#         '''Combine the outputs from the batch jobs'''
#         #### TODO: just touch the output db, since the batch job will write directly to the DB
#         #with self.output().open("wb") as f:
#         #    f.write(json.dumps(outdata).encode('utf-8'))
#         pass


class RootTask(luigi.WrapperTask):
    '''A dummy root task, which collects the database configurations
    and executes the central task. 

    Args:
        date (datetime): Date used to label the outputs
    '''
    date = luigi.DateParameter(default=datetime.date.today())
    iso2 = luigi.Parameter()
    category = luigi.Parameter()

    def requires(self):
        '''Collects the database configurations and executes the central task.'''
        logging.getLogger().setLevel(logging.INFO)
        task_id = "{}-{}-{}".format(self.date, self.iso2, self.category)
        
        yield GroupsMembersTask(iso2=self.iso2,
                                category=self.category,
                                task_id=task_id,
                                batchable=("/home/ec2-user/nesta/production/"
                                           "batchables/meetup/groups_members/"),
                                env_files=["/home/ec2-user/nesta/production/config/mysqldb.config",
                                           "/home/ec2-user/nesta/production/orms/",
                                           "/home/ec2-user/nesta/packages/meetup/"],
                                job_def="py36_amzn1_image",
                                job_name="GroupsMembers-%s" % self.task_id,      
                                job_queue="HighPriority",
                                region_name="eu-west-2",
                                poll_time=60)

        # yield GroupDetailsTask(date=self.date,
        #                        iso2=self.iso2,
        #                        category=self.category,
        #                        in_db_config=in_db_config,
        #                        out_db_config=out_db_config,
        #                        batchable=("/home/ec2-user/nesta/production/"
        #                                   "batchables/meetup/something"),
        #                        job_def="standard_image",
        #                        job_name="GroupDetails-%s" % self.date,
        #                        job_queue="HighPriority",
        #                        region_name="eu-west-2",
        #                        poll_time=60)
