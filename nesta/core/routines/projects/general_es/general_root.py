"""
Root Task (General ES pipelines)
================================

Pipe general data from MySQL to Elasticsearch,
without project-specific enrichment.
"""

from nesta.core.luigihacks.luigi_logging import set_log_level
from nesta.core.luigihacks.sql2estask import Sql2EsTask
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p

from nesta.core.orms.gtr_orm import Projects as GtrProject

from datetime import datetime as dt
import luigi

S3_BUCKET='nesta-production-intermediate'

def kwarg_maker(dataset, routine_id):
    env_files=[f3p('mysqldb.config'),
               f3p('elasticsearch.config'),
               f3p(f'tier_1/datasets/{dataset}.json'),
               f3p('nesta')]
    batchable=f3p(f'batchables/general/{dataset}')
    return dict(dataset=dataset,
                endpoint='general',
                routine_id=f'{routine_id}_{dataset}',
                env_files=env_files,
                batchable=batchable)


class RootTask(luigi.WrapperTask):
    process_batch_size = luigi.IntParameter(default=1000)
    production = luigi.BoolParameter(default=False)
    date = luigi.DateParameter(default=dt.now())
    drop_and_recreate = luigi.BoolParameter(default=False)

    def requires(self):
        test = not self.production
        set_log_level(True)
        routine_id = f'General-Sql2EsTask-{self.date}'
        default_kwargs = dict(date=self.date,
                              process_batch_size=self.process_batch_size,
                              drop_and_recreate=self.drop_and_recreate,
                              job_def='py36_amzn1_image',
                              job_name=routine_id,
                              job_queue='HighPriority',
                              region_name='eu-west-2',
                              poll_time=10,
                              max_live_jobs=300,
                              db_config_env='MYSQLDB',
                              test=test,
                              memory=2048,
                              intermediate_bucket=S3_BUCKET)

        params = (('gtr', 'project', GtrProject.id),)
        for dataset, entity_type, id_field in params:
            yield Sql2EsTask(id_field=id_field,
                             entity_type=entity_type,
                             **kwarg_maker(dataset, 
                                           routine_id),
                             **default_kwargs)