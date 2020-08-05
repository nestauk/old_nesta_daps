from nesta.core.luigihacks.estask import LazyElasticsearchTask
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p
import luigi
from datetime import datetime as dt
import logging

class NiHLolveltyRootTask(luigi.WrapperTask):
    production = luigi.BoolParameter(default=False)
    index = luigi.Parameter(default=None)
    date = luigi.DateParameter(default=dt.now())
    def requires(self):
        logging.getLogger().setLevel(logging.INFO)
        kwargs = {'score_field': '_rank_rhodonite_abstract',
                  'fields': ['title_of_project', 'title_of_organisation',
                             'terms_descriptive_project']}
        if self.production:
            kwargs = {'score_field': 'rank_rhodonite_abstract',
                      'fields': ['textBody_abstract_project']}
        test = not self.production
        routine_id = f"NiHLolveltyTask-{self.date}-{test}"
        index = self.index if self.production else 'nih_dev0'
        assert index is not None
        return LazyElasticsearchTask(routine_id=routine_id,
                                     test=test,
                                     index=index,
                                     dataset='nih',
                                     endpoint='health-scanner',
                                     entity_type='paper',
                                     kwargs=kwargs,
                                     batchable=f3p("batchables/novelty/lolvelty"),
                                     env_files=[f3p("nesta/"),
                                                f3p("config/mysqldb.config"),
                                                f3p("config/elasticsearch.yaml")],
                                     job_def="py36_amzn1_image",
                                     job_name=routine_id,
                                     job_queue="HighPriority",
                                     region_name="eu-west-2",
                                     poll_time=10,
                                     memory=1024,
                                     max_live_jobs=10)
