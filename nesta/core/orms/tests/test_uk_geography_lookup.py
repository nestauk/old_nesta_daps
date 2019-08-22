import unittest
from sqlalchemy.orm import sessionmaker
#from nesta.core.orms.uk_geography_lookup_orm import UkGeographyLookup
from nesta.core.orms.uk_geography_lookup_orm import Base
from nesta.core.orms.orm_utils import get_mysql_engine


class TestUkGeographyLookup(unittest.TestCase):
    '''Check that the WiktionaryNgram ORM works as expected'''
    engine = get_mysql_engine("MYSQLDBCONF", "mysqldb")
    Session = sessionmaker(engine)

    def setUp(self):
        '''Create the temporary table'''
        Base.metadata.create_all(self.engine)

    def tearDown(self):
        '''Drop the temporary table'''
        Base.metadata.drop_all(self.engine)

    def test_build(self):
        pass

if __name__ == "__main__":
    unittest.main()
