from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.luigihacks.autobatch import AutoBatchTask
from nesta.core.orms.orm_utils import setup_es, get_es_ids, get_config
from nesta.packages.misc_utils.batches import split_batches, put_s3_batch
from abc import abstractmethod
import luigi
import logging
import functools


class ElasticsearchTask(AutoBatchTask):
    '''Note: :obj:`done_ids` must be overridden with a function
    return all document ids which do not require processing. If
    you want to avoid writing that function see
    :obj:`LazyElasticsearchTask`.

    Args:
        routine_id (str): Label for this routine.
        db_config_path (str): Database config path.
        endpoint (str): AWS domain name of the ES endpoint.
        dataset (str): Name of the ES dataset.
        entity_type (str): Entity type, for :obj:`ElasticsearchPlus`.
        kwargs (dict): Any extra parameters to pass to the batchables.
        index (str): Override the elasticsearch config with this index.
        process_batch_size (int): Number of documents per batch.
        intermediate_bucket (str): S3 bucket where batch chunks are stored.
        sql_config_filename (str): SQL config path/filename in the batch task.
    '''
    routine_id = luigi.Parameter()
    db_config_path = luigi.Parameter('mysqldb.config')
    endpoint = luigi.Parameter()
    dataset = luigi.Parameter()
    entity_type = luigi.Parameter()
    kwargs = luigi.DictParameter(default={})
    index = luigi.Parameter(default=None)
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
        '''All document ids which do not require processing. If
        you want to avoid writing that function see
        :obj:`LazyElasticsearchTask`.

        Returns:
            done_ids (set): A set of document ids, not to be processed.
        '''
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
        '''Chunk up elasticsearch data, and submit batch
        jobs over those chunks.'''
        if self.test:
            self.process_batch_size = 1000
            logging.warning("Batch size restricted to "
                            f"{self.process_batch_size}"
                            " while in test mode")

        # Setup elasticsearch and extract all ids
        es_mode = 'dev' if self.test else 'prod'
        es, es_config = setup_es(es_mode=es_mode,
                                 dataset=self.dataset,
                                 endpoint=self.endpoint,
                                 drop_and_recreate=False,
                                 increment_version=False)
        ids = get_es_ids(es, es_config, size=10000)  # All ids in this index
        ids = ids - self._done_ids  # Don't repeat done ids

        # Override the default index if specified
        es_config['index'] = (self.index if self.index is not None
                              else es_config['index'])

        # Generate the job params
        job_params = []
        batches = split_batches(ids, self.process_batch_size)
        for count, batch in enumerate(batches, 1):
            done = False  # Already taken care of with _done_ids
            # write batch of ids to s3
            batch_file = ''
            if not done:
                batch_file = put_s3_batch(batch,
                                          self.intermediate_bucket,
                                          self.routine_id)
            params = {
                "batch_file": batch_file,
                "config": self.sql_config_filename,
                "bucket": self.intermediate_bucket,
                "done": done,
                "count": len(ids),
                'outinfo': es_config['host'],
                'out_port': es_config['port'],
                'index': es_config['index'],
                'out_type': es_config['type'],
                'aws_auth_region': es_config['region'],
                'test': self.test,
                'routine_id': self.routine_id,
                'entity_type': self.entity_type,
                **self.kwargs
            }
            job_params.append(params)
            # Test mode
            if self.test and count > 1:
                logging.warning("Breaking after 2 batches "
                                "while in test mode.")
                logging.warning(job_params)
                break
        # Done
        logging.info("Batch preparation completed, "
                     f"with {len(job_params)} batches")
        return job_params

    def combine(self, job_params):
        '''Touch the checkpoint'''
        self.output().touch()


class LazyElasticsearchTask(ElasticsearchTask):
    '''Same as ElasticsearchTask, except no done_ids'''
    def done_ids(self):
        return set()
