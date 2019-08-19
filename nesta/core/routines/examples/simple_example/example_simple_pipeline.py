'''
Simple Example
==============
An example of building a pipeline with just a wrapper task and a regular task.
'''

from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.luigihacks.misctools import get_config

import luigi
import datetime
import os
import logging
import requests


class RootTask(luigi.WrapperTask):
    '''The root task, which collects the supplied parameters and calls the SimpleTask.

    Args:
        date (datetime): Date used to label the outputs
        name (str): name to search for in the swapi
        production (bool): test mode or production mode
    '''
    date = luigi.DateParameter(default=datetime.datetime.today())
    name = luigi.Parameter(default='')
    production = luigi.BoolParameter(default=False)

    def requires(self):
        '''Call the task to run before this in the pipeline.'''

        logging.getLogger().setLevel(logging.INFO)
        return SimpleTask(date=self.date,
                          name=self.name,
                          test=not self.production)


class SimpleTask(luigi.Task):
    '''Collects all starship data from the swapi and prints them to the console.
    If name is supplied, then only matches are returned.

    Args:
        date (datetime): Date used to label the outputs
        name (str): name to search for in the swapi
        test (bool): run a shorter version of the task if in test mode
    '''
    date = luigi.DateParameter(default=datetime.datetime.today())
    name = luigi.Parameter()
    test = luigi.BoolParameter()

    def output(self):
        '''Points to the output database engine where the task is marked as done.
        The luigi_table_updates table exists in test and production databases.
        '''
        db_config = get_config(os.environ["MYSQLDB"], "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Example <dummy>"  # Note, not a real table
        update_id = "SimpleTask_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        base_url = "https://swapi.co/api/starships{}"
        url = base_url.format(f"?search={self.name}" if self.name else '')

        r = requests.get(url)
        r.raise_for_status()

        results = r.json()['results']
        for count, result in enumerate(results, 1):
            print(f"***{result['name']} - {result['model']} - {result['manufacturer']}")

            if count > 1 and self.test:
                logging.info("Breaking after 2 results in test mode")
                break

        # mark as done
        logging.info("Task complete")
        self.output().touch()
