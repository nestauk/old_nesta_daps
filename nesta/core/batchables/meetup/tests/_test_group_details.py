import pytest
from sqlalchemy.orm import sessionmaker
from nesta.core.orms.meetup_orm import Base
from nesta.core.orms.orm_utils import get_mysql_engine
from unittest import mock
from unittest import TestCase
import os

from nesta.core.batchables.meetup import group_details

environ = {"BATCHPAR_group_urlnames":("[b'art@34', b'french@5', b'ibd-214', b'newintown@48', b'Westchester-Singles', b'socialnetwork@113', b'broward-bellydance', b'localpolitics-76', b'bookclub@511', b'women@57', b'newyorkcultureclub', b'TheUrbanTriangle', b'phoenixsingles', b'VegasVolunteers', b'expat-171', b'newintown@822', b'newintown@839', b'newtech@76', b'sdsongwriters', b'NJPARealEstateInvestorsREIAGroup', b'NYProfessionals', b'PBSN-NJ', b'entrepreneur@1775', b'TheOrlandoGirlsNightOutClub', b'entrepreneur@1906', b'ottawa-social', b'MBC-Houston', b'ocphotoshop', b'friends-961', b'www-hiking-ilkley-org', b'_fns_NYC-Web-Devs', b'docwong', b'30s-and-40s-Generation-X-Group', b'_fns_The-Cambridge-Expats-New-in-Town-Meetup-Group', b'AloneinToronto', b'Earning-Passive-Income-Through-Real-Estate', b'Free-QuickBooks-Workshop', b'Models-Makeup-Artists-and-Photographers-Meetup', b'_fns_Closed-Group123', b'NetworkingInPerson', b'NEOWordPress', b'Project-Management-Professionals-Network-Group', b'_fns_BrusselsNightLife', b'getonbase', b'250Kfromhome', b'Duluth-Rocking-Singles', b'JiveNation', b'denver-single-social-girlfriend-connection', b'Sydney-Volleyball-in-the-Park', b'NorthCarolinaInternetMarketers', b'Oregon-Freedom-Network', b'CLOSING-DOWN-RIGHT-NOW-RIGHT-NOW', b'IamHappyProjectOaklandCA', b'London-Over-50s-Friendship-Club', b'HmmmBooks', b'AfricaDmv', b'AllThingsFUN', b'Dinner-Parties-for-Singles', b'Art-of-Living-Canton-to-Farmington-Hills', b'Red-Deer-Business-Networking-Group', b'PersonalDevelopmentLovers', b'www-spiritisminorlando-com', b'I-am-happy-Project-Port-Harcourt', b'oshonewyork', b'Rockland-Bergen-Orange-Westchester-Singles-45-to-65', b'madisonphp', b'GardenPool-org', b'CFIC-Regina', b'NYC-SINGLES-over-40-MOVIE-BRUNCH-DINNER-CLUB', b'The-Gold-Coast-Aus-Single-Parents-Meetup-Group', b'PAIR-site', b'DFWYBP', b'Austin-Social-Escapades', b'Bay-Area-Search', b'UpstateCreativeWriters', b'StartupNewark', b'Building-Trades-Network', b'Singles-Association-of-Long-Island', b'San-Francisco-Bay-Area-Highly-Sensitive-Person-HSP-Group', b'Atlanta-INTJ-adaptees', b'The-Freelance-Jungle', b'Jams-Around-Brisbane', b'SpartaBusinessNetworking', b'The-John-de-Ruiter-Freiburg-Meetup-Group-JDR', b'Grupo-HELA', b'AfricansinDFW', b'Toronto-Network-Marketers', b'Jacksonville-Miniatures-Group', b'Single-Expats-in-Denmark', b'HealthTechnologyForum-DC', b'2c15846d-07c0-407f-99c1-0abebaa8a960']"),
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
    @mock.patch('nesta.core.batchables.meetup.group_details.run.boto3')
    def test_group_details(self, boto3):
        n = group_details.run.run()
        self.assertGreater(n, 0)
