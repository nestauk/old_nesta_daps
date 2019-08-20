from nesta.core.luigihacks.estask import LazyElasticsearchTask
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p
import luigi
import logging
from datetime import datetime as dt

class ArxivLolveltyRootTask(luigi.WrapperTask):
    production = luigi.BoolParameter(default=False)
    date = luigi.DateParameter(default=dt.now())
    def requires(self):
        logging.getLogger().setLevel(logging.INFO)
        kwargs = {'score_field': 'metric_novelty_article',
                  'fields': ['textBody_abstract_article']}
        test = not self.production
        routine_id = f"ArxivLolveltyTask-{self.date}-{test}"
        index = 'arxiv_v0' if self.production else 'arxiv_dev'
        return LazyElasticsearchTask(routine_id=routine_id,
                                     test=test,
                                     index=index,
                                     dataset='arxiv',
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
                                     max_live_jobs=10)
