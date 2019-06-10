'''
Meetup topic discovery task
===========================

Task to automatically discover relevant topics from meetup data,
defined as the most frequently occurring from a set of categories.
'''

import luigi
import datetime
import json

from nesta.production.luigihacks import s3
from nesta.production.orms.orm_utils import get_mysql_engine
from nesta.packages.meetup.meetup_utils import get_members_by_percentile
from nesta.packages.meetup.meetup_utils import get_core_topics


S3PREFIX = "s3://nesta-production-intermediate"


class TopicDiscoveryTask(luigi.Task):
    '''Task to automatically discover relevant topics from meetup data, 
    defined as the most frequently occurring from a set of categories.

    Args:
        db_config_env (str): Environmental variable pointing to the path of the DB config.
        routine_id (str): The routine UID.
        core_categories (list): A list of category_shortnames from which to identify topics.
        members_perc (int): A percentile to evaluate the minimum number of members.
        topic_perc (int): A percentile to evaluate the most frequent topics.
        test (bool): Test mode.
    '''
    db_config_env = luigi.Parameter()
    routine_id = luigi.Parameter()
    core_categories = luigi.ListParameter()
    members_perc = luigi.IntParameter(default=10)
    topic_perc = luigi.IntParameter(default=10)
    test = luigi.BoolParameter(default=True)

    def output(self):
        '''Points to the S3 Target'''
        return s3.S3Target(f"{S3PREFIX}/meetup-topics-{self.routine_id}.json")

    def run(self):
        '''Extract the topics of interest'''
        database = 'dev' if self.test else 'production'
        engine = get_mysql_engine(self.db_config_env, 'mysqldb', database)
        members_limit = get_members_by_percentile(engine, perc=self.members_perc)
        topics = get_core_topics(engine,
                                 core_categories=self.core_categories,
                                 members_limit=members_limit,
                                 perc=self.topic_perc)

        # Write the intermediate output
        with self.output().open('wb') as outstream:
            outstream.write(json.dumps(list(topics)).encode('utf8'))
