'''
Meetup data to elasticsearch
================================

Luigi routine to load the Meetup Group data from MYSQL into Elasticsearch.
'''

import logging
import luigi
import datetime
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub as f3p
from nesta.production.luigihacks.estask import ElasticsearchTask
from nesta.production.orms.arxiv_orm import Article


class EsRootTask(luigi.WrapperTask):
    production = luigi.BoolParameter(default=False)
    date = luigi.DateParameter(default=datetime.datetime.today())
    drop_and_recreate = luigi.BoolParameter(default=False)

    def requires(self):
        logging.getLogger().setLevel(logging.INFO)
        routine_id = f"{self.date}-{self.production}"
        yield ElasticsearchTask(routine_id=routine_id,
                                date=self.date,
                                process_batch_size=10000,
                                drop_and_recreate=self.drop_and_recreate,
                                dataset='arxiv',
                                id_field=Article.id,
                                entity_type='article',
                                db_config_env='MYSQLDB',
                                test=not self.production,
                                intermediate_bucket='nesta-production-intermediate',
                                batchable=f3p("batchables/arxiv/arxiv_elasticsearch"),
                                env_files=[f3p("nesta/"),
                                           f3p("config/mysqldb.config"),
                                           f3p("schema_transformations/arxiv.json"),
                                           f3p("config/elasticsearch.config")],
                                job_def="py36_amzn1_image",
                                job_name=f"ArxivElasticsearchTask-{routine_id}",
                                job_queue="HighPriority",
                                region_name="eu-west-2",
                                poll_time=10,
                                max_live_jobs=100)
