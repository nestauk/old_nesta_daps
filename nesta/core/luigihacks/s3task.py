import luigi
from nesta.production.luigihacks import s3

class S3Task(luigi.ExternalTask):
    s3_path = luigi.Parameter()
    def output(self):
        return s3.S3Target(self.s3_path)
