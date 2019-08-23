'''
S3 Example
==========

An example of building a pipeline with S3 Targets
'''

import luigi
import datetime
import json
from nesta.core.luigihacks import s3
import time

S3PREFIX = "s3://nesta-dev/production_example_"


class InputData(luigi.ExternalTask):
    '''Dummy task acting as the single input data source'''
    def output(self):
        '''Points to the S3 Target'''
        return s3.S3Target(S3PREFIX+'input.json')


class SomeTask(luigi.Task):
    '''An intermediate task which increments the age of the muppets by 1 year.

    Args:
        date (datetime): Date used to label the outputs
    '''
    date = luigi.DateParameter(default=datetime.datetime.today())

    def requires(self):
        '''Gets the input data (json containing muppet name and age)'''
        return InputData()

    def output(self):
        '''Points to the S3 Target'''
        return s3.S3Target(S3PREFIX+"intermediate_output_%s.json" % self.date)

    def run(self):
        '''Increments the muppets' ages by 1'''

        # Sleep to emulate an actual task
        time.sleep(30)

        # Load the data and increment the age
        instream = self.input().open('rb')
        data = json.load(instream)
        for row in data:
            row["age"] += 1

        # Write the intermediate output
        with self.output().open('wb') as outstream:
            outstream.write(json.dumps(data).encode('utf8'))


class FinalTask(luigi.Task):
    '''The root task, which adds the surname 'Muppet'
    to the names of the muppets.

    Args:
        date (datetime): Date used to label the outputs
    '''
    date = luigi.DateParameter(default=datetime.datetime.today())

    def requires(self):
        '''Get data from the intermediate task'''
        return SomeTask(self.date)

    def output(self):
        '''Points to the S3 Target'''
        return s3.S3Target(S3PREFIX+"final_output_%s.json" % self.date)

    def run(self):
        '''Appends 'Muppet' the muppets' names'''

        # Sleep to emulate an actual task
        time.sleep(30)

        # Load the data and append the word 'Muppet'
        instream = self.input().open('rb')
        data = json.load(instream)
        for row in data:
            row["name"] += " Muppet"

        # Write the final output
        with self.output().open('wb') as outstream:
            outstream.write(json.dumps(data).encode('utf8'))
