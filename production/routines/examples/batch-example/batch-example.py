from luigihacks import autobatch
from luigihacks import s3
import luigi


S3PREFIX = "s3://nesta-dev/production_batch_example_"

class InputData(luigi.ExternalTask):
    def output(self):
        return S3Target(S3PREFIX+'input.json')


class SomeBatchTask(autobatch.AutoBatchTask):
    
    def requires(self):
        return SomeInitialTask()

    def output(self):
        return s3.S3Target(S3PREFIX+"intermediate_output_%s.json" % self.date)

    def prepare(self):
        with input().open("rb") as f:
            job_params = something(f)
        for params in job_params:
            params["done"] = False
            params["outinfo"] = outpathinstructions
        return job_params

    def combine(self, job_params):
        outdata = []
        for params in job_params:
            _data = S3Target(params["outinfo"]).open("rb")
            outdata.append(json.loads(_data))

        with output().open("wb") as f:
            f.write(json.dumps(outdata).encode('utf-8'))
    

class FinalTask(luigi.Task):
    date = luigi.DateParameter(default=datetime.datetime.today())

    def requires(self):
        return SomeBatchTask(self.date)

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
