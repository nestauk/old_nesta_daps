import pytest
from sqlalchemy.orm import sessionmaker
from nesta.core.orms.meetup_orm import Base
from nesta.core.orms.orm_utils import get_mysql_engine
from unittest import mock
from unittest import TestCase
import os

from nesta.core.batchables.meetup import country_groups

environ = {"BATCHPAR_coords":("[(3.04197, 36.7525),]"),
                              # " (-2.690922669999983, 18.97638700000003),"
                              # " (1.2932775500000133, 18.97638700000003),"
                              # " (5.27747777000001, 18.97638700000003),"
                              # " (9.261677990000006, 18.97638700000003),"
                              # " (13.245878210000004, 18.97638700000003),"
                              # " (-8.667222999999979, 20.96848711000003),"
                              # " (-4.683022779999982, 20.96848711000003),"
                              # " (-0.6988225599999849, 20.96848711000003),"
                              # " (3.2853776600000124, 20.96848711000003),"
                              # " (7.269577880000009, 20.96848711000003),"
                              # " (11.253778100000005, 20.96848711000003),"
                              # " (-6.67512288999998, 22.96058722000003),"
                              # " (-2.690922669999983, 22.96058722000003),"
                              # " (1.2932775500000133, 22.96058722000003),"
                              # " (5.27747777000001, 22.96058722000003),"
                              # " (9.261677990000006, 22.96058722000003),"
                              # " (13.245878210000004, 22.96058722000003),"
                              # " (-8.667222999999979, 24.952687330000025),"
                              # " (-4.683022779999982, 24.952687330000025)]"),
           "BATCHPAR_name":"Algeria",
           "BATCHPAR_cat":"34",
           "BATCHPAR_iso2":"DZ",
           "BATCHPAR_radius":"188.57589532086934",
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
    @mock.patch('nesta.core.batchables.meetup.country_groups.run.boto3')
    def test_country_groups(self, boto3):
        n = country_groups.run.run()
        self.assertGreater(n, 0)
