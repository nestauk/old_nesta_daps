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
from nesta.production.orms.meetup_orm import Group
from nesta.production.luigihacks.batchgeocode import GeocodeBatchTask
from nesta.production.routines.meetup.health_tagging.topic_discovery_task import TopicDiscoveryTask
from nesta.production.routines.meetup.health_tagging.group_details_task import GroupDetailsTask


class MeetupHealthElasticsearchTask(ElasticsearchTask):
    core_categories = luigi.ListParameter()
    members_perc = luigi.IntParameter()
    topic_perc = luigi.IntParameter()

    def requires(self):
        yield GeocodeBatchTask(_routine_id=self.routine_id,
                               test=self.test,
                               db_config_env=self.db_config_env,
                               city_col=Group.city,
                               country_col=Group.country,
                               country_is_iso2=True,
                               env_files=[f3p("nesta/"),
                                          f3p("config/mysqldb.config"),
                                          f3p("config/crunchbase.config")],
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
        yield MeetupHealthElasticsearchTask(routine_id=routine_id,                                            
                                            date=self.date,
                                            process_batch_size=10000,
                                            insert_batch_size=10000,
                                            drop_and_recreate=self.drop_and_recreate,
                                            aliases='health_scanner',
                                            dataset='meetup',
                                            id_field=Group.id,
                                            entity_type='meetup group',
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
                                            job_name=f"MeetupHealthElasticsearchTask-{routine_id}",
                                            job_queue="HighPriority",
                                            region_name="eu-west-2",
                                            poll_time=10,
                                            memory=2048,
                                            max_live_jobs=100)
