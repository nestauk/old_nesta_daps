import pytest
from sqlalchemy.orm import sessionmaker
from nesta.production.orms.meetup_orm import Base
from nesta.production.orms.orm_utils import get_mysql_engine
from unittest import mock
from unittest import TestCase
import os

from nesta.production.batchables.meetup import members_groups

environ = {"BATCHPAR_member_ids":"[4198106, 7912988, 11475762]",
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
    @mock.patch('nesta.production.batchables.meetup.members_groups.run.boto3')
    def test_members_groups(self, boto3):
        n = members_groups.run.run()
        self.assertGreater(n, 0)
