from nesta.core.luigihacks.estask import ElasticsearchTask
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p
import luigi
import logging
from datetime import datetime as dt
from nesta.core.orms.orm_utils import setup_es, get_es_ids


class ArxivESTokenTask(ElasticsearchTask):
    def done_ids(self):
        es_mode = 'dev' if self.test else 'prod'
        es, es_config = setup_es(es_mode, self.test,
                                 drop_and_recreate=False,
                                 dataset=self.dataset,
                                 increment_version=False)
        field =  "terms_tokens_article"
        ids = get_es_ids(es, es_config, size=10000,
                         query={"query": {"exists": {"field" : field}}})
        return ids



class ArxivTokenRootTask(luigi.WrapperTask):
    production = luigi.BoolParameter(default=False)
    date = luigi.DateParameter(default=dt.now())
    def requires(self):
        logging.getLogger().setLevel(logging.INFO)
        test = not self.production
        routine_id = f"ArxivLolveltyTask-{self.date}-{test}"
        index = 'arxiv_v1' if self.production else 'arxiv_dev'
        return ArxivESTokenTask(routine_id=routine_id,
                                test=test,
                                index=index,
                                dataset='arxiv',
                                entity_type='article',
                                kwargs=kwargs,
                                batchable=f3p("batchables/arxiv/"
                                              "/arxiv_es_tokens"),
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
