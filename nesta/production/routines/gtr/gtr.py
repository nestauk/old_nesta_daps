'''
Country :math:`\rightarrow` Extended groups
===========================================

Starting with a seed country (and Meetup category),
extract all groups in that country and subsequently
find all groups associated with all members of the
original set of groups.
'''

from nesta.packages.gtr.get_gtr_data import read_xml_from_url
from nesta.packages.gtr.get_gtr_data import TOP_URL
from nesta.packages.gtr.get_gtr_data import TOTALPAGES_KEY
from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.luigihacks.misctools import get_config
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub
from nesta.production.luigihacks import autobatch
from nesta.production.luigihacks import s3
import luigi
import datetime
import logging 
import boto3
import os


# Define these globally since they are shared resources
# TODO: consider bundling this into a Singleton
S3 = boto3.resource('s3')
_BUCKET = S3.Bucket("nesta-production-intermediate")
DONE_KEYS = set(obj.key for obj in _BUCKET.objects.all())


class GtrTask(autobatch.AutoBatchTask):
    '''Get all GtR data'''
    date = luigi.DateParameter(default=datetime.date.today())
    page_size = luigi.IntParameter(default=10)
    
    def output(self):
        '''Points to the input database target'''
        db_config = get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "production" if not self.test else "dev"
        db_config["table"] = "gtr_table"
        return MySqlTarget(update_id=self.job_name, **db_config)


    def prepare(self):
        '''Prepare the batch job parameters'''
        # Assertain the total number of pages first             
        projects = read_xml_from_url(TOP_URL, p=1, s=self.page_size)
        total_pages = int(projects.attrib[TOTALPAGES_KEY])

        job_params = []
        for page in range(1, total_pages+1):
            # Check whether the job has been done already
            s3_key = f"{self.job_name}-{page}"
            s3_path = "s3://nesta-production-intermediate/%s" % s3_key
            done = s3_key in DONE_KEYS
            # Fill in the params
            params = {"PAGESIZE":self.page_size,
                      "page": page,
                      "config": "mysqldb.config",
                      "db":"production" if not self.test else "dev",
                      "outinfo":s3_path, "done":done}
            job_params.append(params)
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
    page_size = luigi.IntParameter(default=10)
    production = luigi.BoolParameter(default=False)
    
    def requires(self):
        '''Collects the database configurations and executes the central task.'''
        logging.getLogger().setLevel(logging.INFO)        
        yield GtrTask(date=self.date,
                      page_size=self.page_size,
                      batchable=find_filepath_from_pathstub("production/batchables/gtr/"),
                      env_files=[find_filepath_from_pathstub("/nesta/nesta"),
                                 find_filepath_from_pathstub("/config/mysqldb.config")],
                      job_def="py36_amzn1_image",
                      job_name=f"GtR-{self.date}-{self.page_size}-{self.production}",
                      job_queue="HighPriority",
                      region_name="eu-west-2",
                      poll_time=10,
                      test=(not self.production))
