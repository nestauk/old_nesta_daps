'''                                                            
S3 Example                                                     
==========                                                     
                                                               
An example of building a pipeline with S3 Targets              
'''

import luigi
from nesta.production.luigihacks import s3
from nesta.production.routines.automl.automl import AutoMLTask
import os
import logging

S3INTER = ("s3://clio-data/{dataset}/intermediate/")
S3PREFIX = ("s3://clio-data/{dataset}/{phase}/"
            "{dataset}_{phase}.json")

THIS_PATH = os.path.dirname(os.path.realpath(__file__))
CHAIN_PARAMETER_PATH = os.path.join(THIS_PATH,
                                    "clio_process_task_chain.json")


class ClioProcessTextTask(luigi.WrapperTask):
    dataset = luigi.Parameter()    

    def requires(self):
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("botocore").setLevel(logging.WARNING)
        logging.getLogger("luigi-interface").setLevel(logging.WARNING)
        logging.getLogger("boto3").setLevel(logging.WARNING)

        logging.getLogger().setLevel(logging.DEBUG)

        yield AutoMLTask(s3_path_in=S3PREFIX.format(dataset=self.dataset, 
                                                    phase="raw_data"),
                         s3_path_out=S3PREFIX.format(dataset=self.dataset, 
                                                     phase="processed_data"),
                         s3_path_intermediate=S3INTER.format(dataset=self.dataset),
                         task_chain_filepath=CHAIN_PARAMETER_PATH)
