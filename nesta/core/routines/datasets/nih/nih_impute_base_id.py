'''
Impute Base ID
==============

What NiH don't tell you is that the `core_project_num` field
has a base project number, which is effectively the actual
core project number. This is essential for aggregating projects,
otherwise it will appear that many duplicates exist in the data.

This task imputes these values using a simple regex of the form:

    {BASE_ID}-{an integer}-{an integer}-{an integer}

Any `core_project_num` failing this regex are ignored.
'''


import logging
import luigi
from datetime import datetime as dt
from itertools import repeat
from multiprocessing.dummy import Pool as ThreadPool

from nesta.core.luigihacks.mysqldb import make_mysql_target
from nesta.packages.nih.impute_base_id import retrieve_id_ranges
from nesta.packages.nih.impute_base_id import impute_base_id_thread


class ImputeBaseIDTask(luigi.Task):
    '''Impute the base ID using a regex of the form
    {BASE_ID}-{an integer}-{an integer}-{an integer}

    Args:
        date (datetime): Date stamp.
        test (bool): Running in test mode?
    '''
    date = luigi.DateParameter()
    test = luigi.BoolParameter()

    def output(self):
        return make_mysql_target(self)

    def run(self):
        database = 'dev' if self.test else 'production'
        id_ranges = retrieve_id_ranges(database)
        # Threading required because it takes 2-3 days to impute
        # all values on a single thread, or a few hours on 15 threads
        pool = ThreadPool(15)
        args = zip(id_ranges, repeat(database))
        results = pool.starmap(impute_base_id_thread, args)
        pool.close()
        pool.join()
        self.output().touch()


class RootTask(luigi.WrapperTask):
    date = luigi.DateParameter(default=dt.now())
    production = luigi.BoolParameter(default=False)

    def requires(self):
        yield ImputeBaseIDTask(date=self.date, test=not self.production)
