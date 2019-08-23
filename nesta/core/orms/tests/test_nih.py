import unittest
from sqlalchemy.orm import sessionmaker
from nesta.core.orms.nih_orm import Base
from nesta.core.orms.nih_orm import Projects
from nesta.core.orms.nih_orm import Abstracts
from nesta.core.orms.nih_orm import Publications
from nesta.core.orms.nih_orm import Patents
#from nesta.core.orms.nih_orm import LinkTables
from nesta.core.orms.orm_utils import get_mysql_engine
from sqlalchemy.exc import IntegrityError


class TestMeetup(unittest.TestCase):
    '''Currently just a placeholder test to check that the schema compiles'''
    engine = get_mysql_engine("MYSQLDBCONF", "mysqldb")
    Session = sessionmaker(engine)

    def setUp(self):
        '''Create the temporary table'''
        Base.metadata.create_all(self.engine)

    def tearDown(self):
        '''Drop the temporary table'''
        Base.metadata.drop_all(self.engine)        

    def test_constraints(self):
        '''Placeholder for if any contraints are added'''
        session = self.Session()
        session.close()


if __name__ == "__main__":
    unittest.main()
