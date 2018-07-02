from luigihacks import autobatch
from luigihacks import s3
import luigi
import datetime
import json

S3PREFIX = "s3://nesta-dev/production_batch_example_"

class SomeInitialTask(luigi.ExternalTask):
    def output(self):
        return s3.S3Target(S3PREFIX+'input.json')


class SomeBatchTask(autobatch.AutoBatchTask):
    date = luigi.DateParameter(default=datetime.datetime.today())

    def requires(self):
        return SomeInitialTask()

    def output(self):
        return s3.S3Target(S3PREFIX+"intermediate_output_%s.json" % self.date)

    def prepare(self):
        instream = self.input().open("rb")
        job_params = json.load(instream)
        s3fs = s3.S3FS()
        for i, params in enumerate(job_params):
            params["outinfo"] = ("s3://nesta-production-intermediate/"
                                 "batch-example-{}-{}".format(self.date, i))            
            params["done"] = s3fs.exists(params["outinfo"])
        return job_params

    def combine(self, job_params):
        outdata = []
        for params in job_params:
            _data = s3.S3Target(params["outinfo"]).open("rb")
            outdata.append(json.loads(_data))

        with output().open("wb") as f:
            f.write(json.dumps(outdata).encode('utf-8'))
    

class FinalTask(luigi.Task):
    date = luigi.DateParameter(default=datetime.datetime.today())

    def requires(self):
        return SomeBatchTask(date=self.date,
                             batchable = ("/home/ec2-user/nesta/production/"
                                          "batchables/examples/batch-example/"),
                             job_def = "test_definition",
                             job_name = "batch-example-%s" % self.date,
                             job_queue = "HighPriority",
                             region_name = "eu-west-2",
                             poll_time = 60)

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
