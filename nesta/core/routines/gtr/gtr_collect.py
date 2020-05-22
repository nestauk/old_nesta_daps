'''
Data collection
===============

Discover all GtR data via the API.
'''
import luigi
import datetime
import boto3
import logging

from nesta.packages.gtr.get_gtr_data import read_xml_from_url
from nesta.packages.gtr.get_gtr_data import TOP_URL
from nesta.packages.gtr.get_gtr_data import TOTALPAGES_KEY
from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.luigihacks.misctools import get_config
from nesta.core.luigihacks import autobatch
from nesta.core.luigihacks import s3
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub


# Define these globally since they are shared resources
# TODO: consider bundling this into a Singleton
S3 = boto3.resource('s3')
_BUCKET = S3.Bucket("nesta-production-intermediate")
DONE_KEYS = set(obj.key for obj in _BUCKET.objects.all())


def get_range_by_weekday(total_pages):
    """The range of pages that should be queried today.
    The reason for implementing this is that the GtR server is
    very sensitive to heavy loading, so we distribute the requests
    evenly across the week.

    Args:
        total_pages (int): The total number of pages on GtR to be queried.
    Returns:
        {first_page, last_page} (int,int): Corresponding page range (a, b+1) that should be queried

    """
    weekday = datetime.date.today().weekday()  # values in {0 ... 6}
    pages_per_day = total_pages // 7  # Don't worry about the remainder yet
    first_page = weekday*pages_per_day + 1
    last_page = (total_pages + 1 if weekday == 6  # On Sundays include the remainder
                 else first_page + pages_per_day)
    return first_page, last_page


class GtrTask(autobatch.AutoBatchTask):
    '''
    Get all GtR data
    
    Args:
        date (datetime.datetime): Date for labelling the task.
        page_size (int): Number of pages per batch task.
        split_collection (bool): Automatically split the collection into a daily (n/7th{ish}) chunk?
    '''    
    date = luigi.DateParameter(default=datetime.date.today())
    page_size = luigi.IntParameter(default=10)
    split_collection = luigi.BoolParameter(default=False)

    def output(self):
        '''Points to the input database target'''
        db_config = get_config("mysqldb.config", "mysqldb")
        db_config["database"] = "production" if not self.test else "dev"
        db_config["table"] = "gtr_table"
        return MySqlTarget(update_id=self.job_name, **db_config)

    def prepare(self):
        '''Prepare the batch job parameters'''
        # Ascertain the total number of pages first
        projects = read_xml_from_url(TOP_URL, p=1, s=self.page_size)
        total_pages = int(projects.attrib[TOTALPAGES_KEY])
        if self.split_collection:  # Split data into weekdays
            first_page, last_page = get_range_by_weekday(total_pages)
        else:
            first_page, last_page = (1, total_pages+1)

        job_params = []
        for page in range(first_page, last_page):
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

class GtrOnlyRootTask(luigi.WrapperTask):
    '''A dummy root task, which collects the database configurations
    and executes the central task.

    Args:
        date (datetime): Date used to label the outputs
        page_size (int): Number of pages per batch task.
        split_collection (bool): Automatically split the collection into a daily (n/7th{ish}) chunk?
    '''
    date = luigi.DateParameter(default=datetime.date.today())
    page_size = luigi.IntParameter(default=10)
    split_collection = luigi.BoolParameter(default=False)
    production = luigi.BoolParameter(default=False)

    def requires(self):
        '''Collects the database configurations and executes the central task.'''
        logging.getLogger().setLevel(logging.INFO)
        yield GtrTask(date=self.date,
                      page_size=self.page_size,
                      split_collection=self.split_collection,
                      batchable=find_filepath_from_pathstub("core/batchables/gtr/collect_gtr"),
                      env_files=[find_filepath_from_pathstub("/nesta"),
                                 find_filepath_from_pathstub("/config/mysqldb.config")],
                      job_def="py36_amzn1_image",
                      job_name=f"GtR-{self.date}-{self.page_size}-{self.production}",
                      #job_queue="HighPriority",
                      job_queue="MinimalCpus",
                      region_name="eu-west-2",
                      vcpus=2,
                      poll_time=10,
                      memory=2048,
                      max_live_jobs=10,
                      test=(not self.production))
