"""
Root Task (General ES pipelines)
================================

Pipe general data from MySQL to Elasticsearch,
without project-specific enrichment.
"""

from nesta.core.luigihacks.luigi_logging import set_log_level
from nesta.core.luigihacks.sql2estask import Sql2EsTask
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p

from nesta.core.orms.general_orm import CrunchbaseOrg
from nesta.core.orms.gtr_orm import Projects as GtrProject
from nesta.core.orms.arxiv_orm import Article as ArxivArticle

from datetime import datetime as dt
import luigi

S3_BUCKET='nesta-production-intermediate'
ENV_FILES = ['mysqldb.config', 'elasticsearch.yaml',
             'nesta']
ENDPOINT = 'general'

def kwarg_maker(dataset, routine_id):
    env_files=list(f3p(f) for f in ENV_FILES) + [f3p(f'tier_1/datasets/{dataset}.json')]
    batchable=f3p(f'batchables/general/{dataset}')
    return dict(dataset=dataset,
                endpoint=ENDPOINT,
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
                              job_def='py37_amzn1_es7',
                              job_name=routine_id,
                              job_queue='HighPriority',
                              region_name='eu-west-2',
                              poll_time=10,
                              max_live_jobs=50,
                              db_config_env='MYSQLDB',
                              test=test,
                              memory=2048,
                              intermediate_bucket=S3_BUCKET)

        params = (('gtr', 'project', GtrProject.id),
                  ('arxiv', 'article', ArxivArticle.id),
                  ('companies', 'company', CrunchbaseOrg.id))
        for dataset, entity_type, id_field in params:
            yield Sql2EsTask(id_field=id_field,
                             entity_type=entity_type,
                             **kwarg_maker(dataset, 
                                           routine_id),
                             **default_kwargs)
