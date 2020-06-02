"""
Novelty score (lolvelty)
========================

Apply "lolvelty" score to Meetup data.
"""

from nesta.core.luigihacks.estask import LazyElasticsearchTask
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p
import luigi
from datetime import datetime as dt
import logging

class MeetupLolveltyRootTask(luigi.WrapperTask):
    """Apply Lolvelty score to meetup data.

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
        kwargs = {'score_field': 'rank_rhodonite_group',
                  'fields': ['name_of_group', 'textBody_descriptive_group',
                             'terms_topics_group']}
        test = not self.production
        routine_id = f"MeetupLolveltyTask-{self.date}-{test}"
        index = self.index if self.production else 'meetup_dev'
        assert index is not None
        return LazyElasticsearchTask(routine_id=routine_id,
                                     test=test,
                                     index=index,
                                     dataset='meetup',
                                     endpoint='health-scanner',
                                     entity_type='meetup',
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
