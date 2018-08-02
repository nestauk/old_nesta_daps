'''
Country :math:`\rightarrow` Extended groups
===========================================

Starting with a seed country (and Meetup category),
extract all groups in that country and subsequently
find all groups associated with all members of the
original set of groups.
'''

from luigihacks import autobatch
from luigihacks import s3
import luigi
import datetime
import json
import time


class CountryGroupsTask(autobatch.AutoBatchTask):
    '''Extract all groups with corresponding category for this country.

    Args:
    
    
    '''
    date = luigi.DateParameter()
    iso2 = luigi.Parameter()
    category = luigi.Parameter()

    def output(self):
        '''Points to the input database target'''
        update_id = "meetup_groups_"+str(self.date)
        db_config = misctools.get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "production"
        return MySqlTarget(update_id=update_id, **self.db_config)

    def prepare(self):
        '''Prepare the batch job parameters'''
        # Add the mandatory `outinfo' and `done' fields        
        return [{"date":self.date, "iso2":self.iso2, "cat":category,
                 "outinfo":mysqldb.config, "done":False}]

    def combine(self, job_params):
        '''Combine the outputs from the batch jobs'''
        self.output().touch()


# class BaseGroupsMembersTask(autobatch.AutoBatchTask):
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
#     date = luigi.DateParameter()
#     iso2 = luigi.Parameter()
#     category = luigi.Parameter()    
#     meetup_id = luigi.Parameter()

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


# class GroupsMembersTask(BaseGroupsMembersTask):
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
#         return CountryGroupsTask(self.date, self.iso2, self.category)

#     def output(self):
#         '''Points to the DB target'''
#         #### TODO: Change to DB
#         return s3.S3Target(S3PREFIX+"intermediate_output_%s.json" % self.date)


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
        yield CountryGroupsTask(date=self.date,
                                iso2=self.iso2,
                                category=self.category,
                                batchable=("/home/ec2-user/nesta/production/"
                                           "batchables/meetup/country_groups/"),
                                job_def="standard_image",
                                job_name="CountryGroups-%s" % self.date,      
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
