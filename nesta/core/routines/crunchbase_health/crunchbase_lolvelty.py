"""
Novelty score (lolvelty)
========================

Apply "lolvelty" score to Crunchbase data (in Elasticsearch). Note: this is a slow
procedure that is applied on a document-by-document basis.
"""


from nesta.core.luigihacks.estask import LazyElasticsearchTask
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p
import luigi
from datetime import datetime as dt
import logging

class CrunchbaseLolveltyRootTask(luigi.WrapperTask):
    """Apply Lolvelty score to crunchbase data.

    Args:
        production (bool): Running in full production mode?
        index (str): Elasticsearch index to append Lolvelty score to.
        date (datetime): Date for timestamping this routine.
    """
    production = luigi.BoolParameter(default=False)
    index = luigi.Parameter(default=None)
    date = luigi.DateParameter(default=dt.now())
    def requires(self):
        logging.getLogger().setLevel(logging.INFO)
        kwargs = {'score_field': 'rank_rhodonite_organisation',
                  'fields': ['name_of_organisation',
                             'textBody_descriptive_organisation',
                             'terms_category_organisation']}
        test = not self.production
        routine_id = f"CrunchbaseLolveltyTask-{self.date}-{test}"
        index = self.index if self.production else 'companies_dev'
        assert index is not None
        return LazyElasticsearchTask(routine_id=routine_id,
                                     test=test,
                                     index=index,
                                     dataset='companies',
                                     endpoint='health-scanner',
                                     entity_type='company',
                                     kwargs=kwargs,
                                     batchable=f3p("batchables/novelty/lolvelty"),
                                     env_files=[f3p("nesta/"),
                                                f3p("config/mysqldb.config"),
                                                f3p("config/elasticsearch.config")],
                                     job_def="py36_amzn1_image",
                                     job_name=routine_id,
                                     job_queue="HighPriority",
                                     region_name="eu-west-2",
                                     poll_time=10,
                                     memory=1024,
                                     max_live_jobs=10)
