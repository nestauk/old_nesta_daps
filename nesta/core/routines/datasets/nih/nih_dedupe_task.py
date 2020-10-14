# TODO: update anything here with latest method (e.g. mysqltarget)
# TODO: set default batchable and runtime params where possible
# TODO: update orm, where required, incl lots of indexes
# TODO: update batchable as required
# TODO: write decent tests to check good dq
'''
Deduplication of near duplicates
================================

Remove nears duplicates of projects from the data. Numeric
fields (such as funding) are aggregated together.
'''

import logging
import luigi
import datetime

from nesta.packages.misc_utils.batches import split_batches, put_s3_batch
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p
from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.luigihacks import autobatch
from nesta.core.orms.orm_utils import get_es_ids
from nesta.core.orms.orm_utils import setup_es
from nesta.core.orms.orm_utils import get_config

from nesta.core.routines.nih.nih_data.nih_abstracts_mesh_task import AbstractsMeshTask

class DedupeTask(autobatch.AutoBatchTask):
    '''
    '''
    date = luigi.DateParameter()
    routine_id = luigi.Parameter()
    intermediate_bucket = luigi.Parameter()
    db_config_path = luigi.Parameter()
    process_batch_size = luigi.IntParameter(default=5000)
    drop_and_recreate = luigi.BoolParameter(default=False)

    def output(self):
        '''Points to the output database engine'''
        db_config = get_config(self.db_config_path,
                               "mysqldb")
        db_config["database"] = ('dev' if self.test
                                 else 'production')
        db_config["table"] = f"{self.routine_id} <dummy>"  # Fake table
        update_id = f"NiHDedupeTask-{self.routine_id}_{self.date}"
        return MySqlTarget(update_id=update_id, **db_config)

    def requires(self):
        yield AbstractsMeshTask(date=self.date,
                                drop_and_recreate=self.drop_and_recreate,
                                _routine_id=self.routine_id,
                                db_config_path=self.db_config_path,
                                test=self.test,
                                batchable=f3p("batchables/nih/"
                                              "nih_abstract_mesh_data"),
                                env_files=[f3p("nesta/"),
                                           f3p("config/mysqldb.config"),
                                           f3p("config/elasticsearch.yaml"),
                                           f3p("nih.json")],
                                job_def=self.job_def,
                                job_name="AbstractsMeshTask-%s" % self.routine_id,
                                job_queue=self.job_queue,
                                region_name=self.region_name,
                                poll_time=self.poll_time,
                                memory=self.memory,
                                max_live_jobs=50)


    def prepare(self):
        if self.test:
            self.process_batch_size = 1000
            logging.warning("Batch size restricted to "
                            f"{self.process_batch_size}"
                            " while in test mode")

        es_kwargs = dict(endpoint='health-scanner',
                         dataset='nih', production=not self.test)
        _, _old_config = setup_es(**es_kwargs)
        es, es_config = setup_es(drop_and_recreate=self.drop_and_recreate,
                                 increment_version=True, **es_kwargs)

        # Count articles from the old index
        logging.info(f"Collected article IDs...")
        _ids = get_es_ids(es, _old_config, size=10000)
        logging.info(f"Collected {len(_ids)} IDs")
        done_ids = get_es_ids(es, es_config, size=10000)

        # Generate the job params
        job_params = []
        batches = split_batches(_ids, self.process_batch_size)
        for count, batch in enumerate(batches, 1):
            # Magical '0.3' is the lower end of the deduplication
            # fraction found by inspection
            done = sum(_id in done_ids
                       for _id in batch) / len(batch) > 0.3
            # write batch of ids to s3
            batch_file = ''
            if not done:
                batch_file = put_s3_batch(batch,
                                          self.intermediate_bucket,
                                          self.routine_id)
            params = {
                "batch_file": batch_file,
                "config": 'mysqldb.config',
                "bucket": self.intermediate_bucket,
                "done": done,
                'outinfo': es_config['host'],
                'out_port': es_config['port'],
                'out_index': es_config['index'],
                'in_index': _old_config['index'],
                'out_type': es_config['type'],
                'aws_auth_region': es_config['region'],
                'entity_type': 'paper',
                'test': self.test,
                'routine_id': self.routine_id
            }

            job_params.append(params)
            if self.test and count > 1:
                logging.warning("Breaking after 2 batches "
                                "while in test mode.")
                logging.warning(job_params)
                break
        logging.info("Batch preparation completed, "
                     f"with {len(job_params)} batches")
        return job_params


    def combine(self, job_params):
        '''Touch the checkpoint'''
        self.output().touch()
