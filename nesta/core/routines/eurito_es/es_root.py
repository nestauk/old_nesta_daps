"""
Root Task (EURITO)
==================

Pipe data from MySQL to Elasticsearch, for use with :obj:`clio-lite`.
"""

from nesta.core.luigihacks.luigi_logging import set_log_level
from nesta.core.luigihacks.sql2estask import Sql2EsTask
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p

from nesta.core.orms.arxiv_orm import Article
from nesta.core.orms.crunchbase_orm import Organization
from nesta.core.orms.patstat_eu_orm import ApplnFamily
from nesta.core.orms.cordis_orm import Project

from datetime import datetime as dt
import luigi

S3_BUCKET='nesta-production-intermediate'

def kwarg_maker(dataset, routine_id):
    env_files=[f3p('config/mysqldb.config'),
               f3p('config/elasticsearch.config'),
               f3p('schema_transformations/eurito/'),
               f3p('nesta')]
    batchable=f3p(f'batchables/eurito/{dataset}_eu')
    return dict(dataset=f'{dataset}-eu',
                routine_id=f'{dataset}-eu_{routine_id}',
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
        routine_id = f'EURITO-ElasticsearchTask-{self.date}-{test}'
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

        params = (('arxiv', 'article', Article.id),
                  ('crunchbase', 'company', Organization.id),
                  ('patstat', 'patent', ApplnFamily.docdb_family_id),
                  ('cordis', 'project', Project.rcn),)

        for dataset, entity_type, id_field in params:
            yield Sql2EsTask(id_field=id_field,
                             entity_type=entity_type,
                             **kwarg_maker(dataset, 
                                           routine_id),
                             **default_kwargs)
