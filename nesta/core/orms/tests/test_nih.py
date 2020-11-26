import unittest
from sqlalchemy.orm import sessionmaker
from nesta.core.orms.nih_orm import Base
from nesta.core.orms.nih_orm import Projects
from nesta.core.orms.nih_orm import Abstracts
from nesta.core.orms.nih_orm import Publications
from nesta.core.orms.nih_orm import Patents
#from nesta.core.orms.nih_orm import LinkTables
from nesta.core.orms.nih_orm import getattr_
from nesta.core.orms.orm_utils import get_mysql_engine
from sqlalchemy.exc import IntegrityError

def test__getattr():
    class AttributeDummy:
        def __init__(self, a):
            self.a = a

    class Dummy:
        list_of_attrs = [AttributeDummy(1), AttributeDummy(7), 
                         AttributeDummy(5)]
        empty_list = []
        null_attr = None
        single_attr = AttributeDummy('abc')

    assert getattr_(Dummy.list_of_attrs, 'a') == [1, 7, 5]
    assert getattr_(Dummy.empty_list, 'a') == None
    assert getattr_(Dummy.null_attr, 'a') == None
    assert getattr_(Dummy.single_attr, 'a') == 'abc'


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
