import unittest

from nesta.packages.meetup.meetup_utils import flatten_data
from nesta.packages.meetup.meetup_utils import get_members_by_percentile
from nesta.packages.meetup.meetup_utils import get_core_topics

from sqlalchemy.orm import sessionmaker
from nesta.core.orms.meetup_orm import Base
from nesta.core.orms.meetup_orm import Group
from nesta.core.orms.orm_utils import get_mysql_engine

class TestMeetupGetters(unittest.TestCase):
    '''Check that the meetup ORM getters work as expected'''
    engine = get_mysql_engine("MYSQLDBCONF", "mysqldb")
    Session = sessionmaker(engine)

    def setUp(self):
        '''Create the temporary table'''
        Base.metadata.create_all(self.engine)

    def tearDown(self):
        '''Drop the temporary table'''
        Base.metadata.drop_all(self.engine)

    def test_get_members_by_percentile(self):
        session = self.Session()
        N = 100
        for i in range(1, N+1):
            group = Group(id=i, members=i)
            session.add(group)
            session.commit()

        n = get_members_by_percentile(self.engine, perc=10)
        assert n < N
        assert n > 1

    def test_get_core_topics(self):
        session = self.Session()
        N = 100
        for i in range(1, N+1):            
            group = Group(id=i, members=i, 
                          category_shortname='dummy',
                          topics=[{"name": str(j) for j in range(i)}])
            session.add(group)
            session.commit()

        topics = get_core_topics(self.engine, core_categories=['dummy'],
                                 members_limit=50, perc=50)
        assert len(topics) > 1
        assert len(topics) < N


class TestMeetupUtils(unittest.TestCase):
    data = [{"key":"value", "key2":{"key3":"value"}},
            {"key":"value", "key2":{"key4":"value"}}]
    keys = ["key", ("key2","key3"), ("key2","key4")]

    def test_flatten(self):
        '''Test that the an error is raised if no group is found'''
        flattened = flatten_data(TestMeetupUtils.data, 
                                 TestMeetupUtils.keys)
        # Test that the number of rows are equal
        self.assertEqual(len(flattened), len(TestMeetupUtils.data))

        # Test that the keys look correct
        joined_keys = [k if type(k) == str else "_".join(k)
                       for k in TestMeetupUtils.keys]
        for row in flattened:
            self.assertGreater(len(row.keys()), 0)
            for k in row.keys():
                self.assertTrue(k in joined_keys)




if __name__ == '__main__':
    unittest.main()
