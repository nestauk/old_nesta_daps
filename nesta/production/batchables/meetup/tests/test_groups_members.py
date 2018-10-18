import pytest
from sqlalchemy.orm import sessionmaker
from nesta.production.orms.meetup_orm import Base
from nesta.production.orms.orm_utils import get_mysql_engine
from unittest import mock
from unittest import TestCase
import os

from nesta.production.batchables.meetup import groups_members

environ = {"BATCHPAR_group_urlname":"uvs-algiers",
           "BATCHPAR_group_id":"19811679",
           "BATCHPAR_cat":"34",
           "BATCHPAR_outinfo":("s3://nesta-production-intermediate/DUMMY"),
           "BATCHPAR_db":"production_tests",
           "BATCHPAR_config": os.environ["MYSQLDBCONF"],
           "MEETUP_API_KEYS": os.environ["MEETUP_API_KEYS"]}


class TestRun(TestCase):

    engine = get_mysql_engine("MYSQLDBCONF", "mysqldb")
    Session = sessionmaker(engine)

    def setUp(self):
        '''Create the temporary table'''
        Base.metadata.create_all(self.engine)

    def tearDown(self):
        '''Drop the temporary table'''
        Base.metadata.drop_all(self.engine)

    @mock.patch.dict(os.environ, environ)
    @mock.patch('nesta.production.batchables.meetup.groups_members.run.boto3')
    def test_groups_members(self, boto3):
        n = groups_members.run.run()
        self.assertGreater(n, 0)
