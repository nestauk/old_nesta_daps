'''
World reporter data collection [DEPRECATED]
===========================================


'''

#from luigihacks import misctools
from luigihacks import autobatch
from luigihacks import s3
#from luigi.contrib.esindex import ElasticsearchTarget
import luigi
import datetime
import logging 
#from io import StringIO
from io import BytesIO
#from pairing import pair
import re
import pandas as pd
import boto3
import json

# Define these globally since they are shared resources
RE_COMP = re.compile(("https://worldreport.nih.gov:443/app/#!/"
                      "researchOrgId=(\w+)&programId=(\w+)"))

S3 = boto3.resource('s3')
_BUCKET = S3.Bucket("nesta.core-intermediate")
DONE_KEYS = set(obj.key for obj in _BUCKET.objects.all())

# def create_key(url):
#     '''Create a composite key based on the abstract url'''
#     ids = [int(i) for i in RE_COMP.findall(url)[0]]
#     return pair(*ids)


def create_pid(url):
    '''Extract the programID field from the abstract url'''
    _, pid = [int(i) for i in RE_COMP.findall(url)[0]]
    return pid 


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


class InputData(luigi.ExternalTask):
    '''Dummy task acting as the single input data source'''
    def output(self):
        '''Points to the S3 Target'''
        return s3.S3Target('s3://nesta-inputs/world_reporter_inputs.csv')


class WorldReporterAbstracts(autobatch.AutoBatchTask):
    '''
    '''
    routine_id = luigi.Parameter()
    chunksize = luigi.IntParameter()
    
    def requires(self):
        '''Gets the input data'''
        return InputData()

    def output(self):
        '''Points to the input database target'''
        return s3.S3Target("s3://nesta-dev/production_"
                           "intermediate_%s.json" % self.routine_id)

    def prepare(self):
        '''Prepare the batch job parameters'''

        # Get the input data
        data = self.input().open("rb")
        df = pd.read_csv(BytesIO(data.read()))
        program_ids = set(df["Abstract Link"].apply(create_pid))
        logging.info("Got {} rows from S3".format(len(program_ids)))

        # Generate the batch parameters
        job_params = []
        n_done = 0
        for i, chunk in enumerate(chunks(sorted(program_ids), self.chunksize)):
            # Generate keys and paths for this chunk
            s3_key = ("batch-worldreporter-abstracts-"
                      "{}-{}".format(self.routine_id, i))
            
            # Otherwise prepare the job for submission
            out_file = "{}.json".format(s3_key)
            out_path = "s3://nesta.core-intermediate/{}".format(out_file)
            done = out_file in DONE_KEYS
            n_done += int(done)
            
            # Fill in the params
            params = {"data": str(chunk),
                      "outinfo": out_path, 
                      "done": done}
            job_params.append(params)

        logging.info("Got {} jobs".format(len(job_params)))
        logging.info("Already done {} jobs".format(n_done))
        return job_params


    def combine(self, job_params):
        '''Combine the outputs from the batch jobs'''
        outdata = []
        for params in job_params:
            _body = s3.S3Target(params["outinfo"]).open("rb")
            _data = _body.read().decode('utf-8')
            outdata.append(json.loads(_data))
        with self.output().open("wb") as f:
            f.write(json.dumps(outdata).encode('utf-8'))


class ProcessData(luigi.Task):
    
    '''Process the data for ElasticSearch compatibility.

    Args: 
        routine_id (str): Date used to label the outputs                                                
    '''

    date = luigi.DateParameter(default=datetime.date.today())
    chunksize = luigi.IntParameter(default=1000)
    production = luigi.BoolParameter(default=False)

    def requires(self):
        logging.getLogger().setLevel(logging.INFO)
        routine_id = "{}-{}-{}".format(self.date,
                                     self.chunksize,
                                     self.production)

        return [
            # Submit WorldReporterAbstract
            WorldReporterAbstracts(routine_id=routine_id, 
                                   chunksize=self.chunksize,
                                   batchable=("/home/ec2-user/nesta.core/"
                                              "batchables/health_data/world_reporter_api/"),
                                   env_files=["/home/ec2-user/nesta.core/config/es.config",
                                              "/home/ec2-user/nesta/packages/health_data/world_reporter_api.py",
                                              "/home/ec2-user/nesta/packages/nlp_utils/"],
                                   job_def="py36_amzn1_image",
                                   job_name="WorldReporterAPI-%s" % routine_id,
                                   job_queue="HighPriority",
                                   region_name="eu-west-2",
                                   success_rate=0.8,
                                   poll_time=10,
                                   max_live_jobs=20,
                                   test=(not self.production)),
            # Submit ProcessData
            ProcessData()
        ]
