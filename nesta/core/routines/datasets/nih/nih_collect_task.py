# TODO: set default batchable and runtime params where possible
# TODO: update orm, where required, incl lots of indexes
# TODO: update batchable to collect and clean as required
# TODO: write decent tests to check good dq
'''
Data collection
===============

Luigi routine to collect NIH World RePORTER data
via the World ExPORTER data dump.
'''

import luigi
import datetime
import logging
import boto3

from nesta.packages.nih.collect_nih import get_data_urls
from nesta.core.luigihacks.mysqldb import make_mysql_target
from nesta.core.luigihacks import autobatch
from nesta.core.luigihacks.misctools import bucket_keys, f3p

OUTBUCKET = 'nesta-production-intermediate'


class CollectTask(autobatch.AutoBatchTask):
    '''Scrape CSVs from the World ExPORTER site and dump the
    data in the MySQL server.'''    
    batchable = luigi.Parameter(default=f3p("batchables/nih/nih_collect_data"))
    env_files = luigi.ListParameter(default=[f3p("nesta/"),
                                             f3p("config/mysqldb.config")])
    job_def = luigi.Parameter(default="py37_amzn2")
    job_name = luigi.Parameter(default="CollectTask")
    job_queue = luigi.Parameter(default='HighPriority')
    region_name = luigi.Parameter(default='eu-west-2')
    memory = luigi.IntParameter(default=2048)
    max_live_jobs = luigi.IntParameter(default=2)

    def output(self):
        '''Points to the output database engine'''
        return make_mysql_target(self)

    def prepare(self):
        '''Prepare the batch job parameters'''
        # Iterate over all tabs
        job_params = []
        for i in range(0, 4):
            logging.info("Extracting table {}...".format(i))
            title, urls = get_data_urls(i)
            table_name = "nih_{}".format(title.replace(" ","").lower())
            for url in urls:
                done = url in bucket_keys(OUTBUCKET)  # Note: lru_cached
                params = {"table_name": table_name,
                          "url": url,
                          "config": "mysqldb.config",
                          "db_name": "production" if not self.test else "dev",
                          "outinfo": f"s3://{OUTBUCKET}/{url}",
                          "done": done}
                job_params.append(params)
        return job_params

    def combine(self, job_params):
        '''Touch the checkpoint'''
        self.output().touch()


class RootTask(luigi.WrapperTask):
    from datetime import datetime as dt
    date = luigi.DateParameter(default=dt.today())
    production = luigi.BoolParameter(default=False)
    def requires(self):
        yield CollectTask(test=not self.production,
                          date=self.date)
