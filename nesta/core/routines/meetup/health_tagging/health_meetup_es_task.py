'''
Pipe to elasticsearch
=====================

Luigi routine to load the Meetup Group data from MySQL into Elasticsearch.
'''

import logging
import luigi
import datetime
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p
from nesta.core.luigihacks.sql2estask import Sql2EsTask
from nesta.core.orms.meetup_orm import Group
from nesta.core.luigihacks.batchgeocode import GeocodeBatchTask
from nesta.core.routines.meetup.health_tagging.topic_discovery_task import TopicDiscoveryTask


class MeetupHealthSql2EsTask(Sql2EsTask):
    '''Task to pipe meetup data to ES. For other arguments, see :obj:`Sql2EsTask`.

    Args:
        core_categories (list): A list of category_shortnames from which to identify topics.
        members_perc (int): A percentile to evaluate the minimum number of members.
        topic_perc (int): A percentile to evaluate the most frequent topics.
    '''
    core_categories = luigi.ListParameter()
    members_perc = luigi.IntParameter()
    topic_perc = luigi.IntParameter()

    def requires(self):
        yield GeocodeBatchTask(_routine_id=self.routine_id,
                               test=self.test,
                               test_limit=None,
                               db_config_env=self.db_config_env,
                               city_col=Group.city,
                               country_col=Group.country,
                               country_is_iso2=True,
                               env_files=[f3p("nesta/"),
                                          f3p("config/mysqldb.config")],
                               job_def="py36_amzn1_image",
                               job_name=f"HealthMeetupGeocodeBatchTask-{self.routine_id}",
                               job_queue="HighPriority",
                               region_name="eu-west-2",
                               poll_time=10,
                               memory=4096,
                               max_live_jobs=2)

        yield TopicDiscoveryTask(routine_id=self.routine_id,
                                 core_categories=self.core_categories,
                                 members_perc=self.members_perc,
                                 topic_perc=self.topic_perc,
                                 db_config_env=self.db_config_env,
                                 test=self.test)


class RootTask(luigi.WrapperTask):
    production = luigi.BoolParameter(default=False)
    date = luigi.DateParameter(default=datetime.datetime.today())
    core_categories = luigi.ListParameter(default=["community-environment",
                                                   "health-wellbeing",
                                                   "fitness"])
    members_perc = luigi.IntParameter(default=10)
    topic_perc = luigi.IntParameter(default=99)
    db_config_env = luigi.Parameter(default="MYSQLDB")
    drop_and_recreate = luigi.BoolParameter(default=False)

    def requires(self):
        logging.getLogger().setLevel(logging.INFO)
        routine_id = (f"{self.date}-{'--'.join(self.core_categories)}"
                      f"-{self.members_perc}-{self.topic_perc}-{self.production}")
        yield MeetupHealthSql2EsTask(routine_id=routine_id,
                                     date=self.date,
                                     process_batch_size=100,
                                     drop_and_recreate=self.drop_and_recreate,
                                     dataset='meetup',
                                     endpoint='health-scanner',
                                     id_field=Group.id,
                                     entity_type='meetup',
                                     core_categories=self.core_categories,
                                     members_perc=self.members_perc,
                                     topic_perc=self.topic_perc,
                                     db_config_env=self.db_config_env,
                                     test=not self.production,
                                     intermediate_bucket='nesta-production-intermediate',
                                     batchable=f3p("batchables/meetup/topic_tag_elasticsearch"),
                                     env_files=[f3p("nesta/"),
                                                f3p("config/mysqldb.config"),
                                                f3p("schema_transformations/meetup.json"),
                                                f3p("config/elasticsearch.config")],
                                     job_def="py36_amzn1_image",
                                     job_name=f"MeetupHealthSql2EsTask-{routine_id}",
                                     job_queue="MinimalCpus",
                                     region_name="eu-west-2",
                                     poll_time=10,
                                     memory=2048,
                                     vcpus=2,
                                     max_live_jobs=100,
                                     kwargs={"members_perc": self.members_perc})
