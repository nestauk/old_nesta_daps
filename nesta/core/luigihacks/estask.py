from nesta.core.luigihacks.autobatch import AutoBatchTask
from abc import abstractmethod

class ElasticsearchTask(AutoBatchTask):
    routine_id = luigi.Parameter()
    db_config_path = luigi.Parameter()
    dataset = luigi.Parameter()
    kwargs = luigi.DictParameter(default={})
    process_batch_size = luigi.IntParameter(default=5000)    
    intermediate_bucket = luigi.Parameter('nesta-production'
                                          '-intermediate')
    sql_config_filename = luigi.Parameter('mysqldb.config')

    @property
    @functools.lru_cache()
    def _done_ids(self):
        return self.done_ids()

    @abstractmethod
    def done_ids(self):
        pass

    def output(self):
        '''Points to the output database engine'''
        _id = self.routine_id
        db_config = get_config(self.db_config_path, "mysqldb")
        db_config["database"] = ('dev' if self.test
                                 else 'production')
        db_config["table"] = f"{_id} <dummy>"  # Fake table
        update_id = f"{_id}_ElasticsearchTask"
        return MySqlTarget(update_id=update_id, **db_config)

    def prepare(self):
        if self.test:
            self.process_batch_size = 1000
            logging.warning("Batch size restricted to "
                            f"{self.process_batch_size}"
                            " while in test mode")

        es_mode = 'dev' if self.test else 'prod'
        es, es_config = setup_es(es_mode, self.test,
                                 drop_and_recreate=False,
                                 dataset=self.dataset,
                                 increment_version=False)    

        ids = get_es_ids(es, _old_config, size=10000)

        # Generate the job params                             
        job_params = []
        batches = split_batches(_ids, self.process_batch_size)
        for count, batch in enumerate(batches, 1):
            done = _id in self.done_ids
            # write batch of ids to s3                             
            batch_file = ''
            if not done:
                batch_file = put_s3_batch(batch,
                                          self.intermediate_bucket,
                                          self.routine_id))
            params = {
                "batch_file": batch_file,
                "config": self.sql_config_filename,
                "bucket": self.intermediate_bucket,
                "done": done,
                "count": len(ids),
                'outinfo': es_config['host'],
                'out_port': es_config['port'],
                'out_index': es_config['index'],
                'in_index': es_config['old_index'],
                'out_type': es_config['type'],
                'aws_auth_region': es_config['region'],
                'test': self.test,
                'routine_id': self.routine_id,
                **self.kwargs
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
