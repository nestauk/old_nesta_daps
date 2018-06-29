import luigi
import datetime
import json
from luigihacks import s3 
import time


S3PREFIX = "s3://nesta-dev/production_example_"

class InputData(luigi.ExternalTask):
    def output(self):
        return s3.S3Target(S3PREFIX+'input.json')

class SomeTask(luigi.Task):
    date = luigi.DateParameter(default=datetime.datetime.today())

    def requires(self):
        return InputData()

    def output(self):
        return s3.S3Target(S3PREFIX+"intermediate_output_%s.json" % self.date)

    def run(self):
        time.sleep(30)
        instream = self.input().open('rb')
        data = json.load(instream)
        for row in data:
            row["age"] += 1

        with self.output().open('wb') as outstream:
            outstream.write(json.dumps(data).encode('utf8'))

class FinalTask(luigi.Task):
    date = luigi.DateParameter(default=datetime.datetime.today())

    def requires(self):
        return SomeTask(self.date)

    def output(self):
        return s3.S3Target(S3PREFIX+"final_output_%s.json" % self.date)

    def run(self):
        time.sleep(30)
        instream = self.input().open('rb')
        data = json.load(instream)
        for row in data:
            row["name"] += " Muppet"

        with self.output().open('wb') as outstream:
            outstream.write(json.dumps(data).encode('utf8'))
