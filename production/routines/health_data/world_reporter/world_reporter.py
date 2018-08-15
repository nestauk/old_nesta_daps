'''
World reporter data collection
===========================================


'''

from health_data.world_reporter import get_csv_data
from luigihacks import misctools
from luigihacks import autobatch
from luigihacks import s3
from luigi.contrib.esindex import ElasticsearchTarget
import luigi
import datetime
import time
import logging 
import boto3


# Define these globally since they are shared resources
# TODO: consider bundling this into a Singleton
S3 = boto3.resource('s3')
_BUCKET = S3.Bucket("nesta-production-intermediate")
DONE_KEYS = set(obj.key for obj in _BUCKET.objects.all())


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


class WorldReporter(autobatch.AutoBatchTask):
    '''    
    
    '''
    date = luigi.DateParameter()
    chunksize = luigi.IntParameter(default=100)
    _routine_id = luigi.Parameter()

    def output(self):
        '''Points to the input database target'''
        update_id = "worldreporter-%s" % self._routine_id
        db_config = misctools.get_config("es.config", "es")
        db_config["index"] = "rwjf"
        db_config["doc_type"] = "fundapp"
        return ElasticsearchTarget(update_id=update_id, **db_config)


    def prepare(self):
        '''Prepare the batch job parameters'''

        df = get_csv_data()
        job_params = []
        for chunk in chunks(df, self.chunksize):
            # Check whether the job has been done already
            ids = chunk.id.values
            s3_key = "{}-{}-{}".format(self.job_name, ids[0], ids[-1])
            s3_in_path = "s3://nesta-inputs/%s" % s3_key
            s3_out_path = "s3://nesta-production-intermediate/%s" % s3_key
            done = s3_key in DONE_KEYS
            # Fill in the params
            params = {"in_path":s3_in_path,
                      #"config":"es.config",
                      "outinfo":s3_out_path, "done":done}
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

    date = luigi.DateParameter()
    chunksize = luigi.IntParameter(default=100)
    production = luigi.BoolParameter(default=False)
    
    def requires(self):
        '''Collects the database configurations and executes the central task.'''
        logging.getLogger().setLevel(logging.INFO)
        _routine_id = "{}-{}-{}".format(self.date,
                                        self.chunksize,
                                        self.production)
        
        yield WorldReporter(date=self,date
                            chunksize=self.chunksize,
                            production=self.production,
                            _routine_id=_routine_id,
                            batchable=("/home/ec2-user/nesta/production/"
                                       "batchables/health_data/world_reporter/"),
                            env_files=["/home/ec2-user/nesta/production/config/es.config",
                                       "/home/ec2-user/nesta/packages/health_data/"],
                            job_def="py35_ubuntu_chrome",
                            job_name="WorldReporter-%s" % _routine_id,
                            job_queue="HighPriority",
                            region_name="eu-west-2",
                            poll_time=10,                        
                            test=(not self.production))
