"""
Estimate novelty (lolvelty)
---------------------------

Estimate the novelty of each article via the :obj:`lolvelty` algorithm.
This is performed on a document-by-document basis and is regrettably
very slow since it is computationally very expensive for the Elasticsearch
server.
"""

from nesta.core.luigihacks.estask import ElasticsearchTask
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p
import luigi
import logging
from datetime import datetime as dt
from nesta.core.orms.orm_utils import setup_es, get_es_ids
from nesta.core.orms.arxiv_orm import Article
from nesta.core.luigihacks.parameter import DictParameterPlus
from nesta.core.routines.arxiv.arxiv_es_task import ArxivESTask


class ArxivElasticsearchTask(ElasticsearchTask):
    date = luigi.DateParameter(default=dt.today())
    drop_and_recreate = luigi.BoolParameter(default=False)
    grid_task_kwargs = DictParameterPlus(default={})

    def done_ids(self):
        es, es_config = setup_es(endpoint=self.endpoint,
                                 dataset=self.dataset,
                                 production=not self.test,
                                 drop_and_recreate=False,
                                 increment_version=False)
        field =  "metric_novelty_article"
        ids = get_es_ids(es, es_config, size=10000,
                         query={"query": {"exists": {"field" : field}}})
        return ids

    def requires(self):
        yield ArxivESTask(routine_id=self.routine_id,
                          date=self.date,
                          grid_task_kwargs=self.grid_task_kwargs,
                          process_batch_size=10000,
                          drop_and_recreate=self.drop_and_recreate,
                          dataset='arxiv',
                          endpoint='arxlive',
                          id_field=Article.id,
                          filter=Article.article_source == 'arxiv',
                          entity_type='article',
                          db_config_env='MYSQLDB',
                          test=self.test,
                          intermediate_bucket=('nesta-production'
                                               '-intermediate'),
                          batchable=f3p('batchables/arxiv/'
                                        'arxiv_elasticsearch'),
                          env_files=[f3p('nesta/'),
                                     f3p('config/'
                                         'mysqldb.config'),
                                     f3p('schema_transformations/'
                                         'arxiv.json'),
                                     f3p('config/'
                                         'elasticsearch.config')],
                          job_def='py36_amzn1_image',
                          job_name=self.routine_id,
                          job_queue='HighPriority',
                          region_name='eu-west-2',
                          memory=2048,
                          poll_time=10,
                          max_live_jobs=100)


class _ArxivElasticsearchTask(ArxivElasticsearchTask):
    def requires(self):
        pass


class ArxivLolveltyRootTask(luigi.WrapperTask):
    production = luigi.BoolParameter(default=False)
    date = luigi.DateParameter(default=dt.now())
    def requires(self):
        logging.getLogger().setLevel(logging.INFO)
        kwargs = {'score_field': 'metric_novelty_article',
                  'fields': ['textBody_abstract_article']}
        test = not self.production
        routine_id = f"ArxivLolveltyTask-{self.date}-{test}"
        index = 'arxiv_v3' if self.production else 'arxiv_dev'
        return _ArxivElasticsearchTask(routine_id=routine_id,
                                       test=test,
                                       index=index,
                                       dataset='arxiv',
                                       endpoint='arxlive',
                                       entity_type='article',
                                       kwargs=kwargs,
                                       batchable=f3p("batchables/novelty"
                                                     "/lolvelty"),
                                       env_files=[f3p("nesta/"),
                                                  f3p("config/mysqldb.config"),
                                                  f3p("config/"
                                                      "elasticsearch.config")],
                                       job_def="py36_amzn1_image",
                                       job_name=routine_id,
                                       job_queue="HighPriority",
                                       region_name="eu-west-2",
                                       poll_time=10,
                                       memory=1024,
                                       max_live_jobs=30)
