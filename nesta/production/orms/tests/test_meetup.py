import unittest
from sqlalchemy.orm import sessionmaker
from nesta.production.orms.meetup_orm import Base
from nesta.production.orms.meetup_orm import Group
from nesta.production.orms.meetup_orm import GroupMember
from nesta.production.orms.orm_utils import get_mysql_engine
from sqlalchemy.exc import IntegrityError

class TestMeetup(unittest.TestCase):
    '''Check that the meetup ORM works as expected'''
    engine = get_mysql_engine("MYSQLDBCONF", "mysqldb")
    Session = sessionmaker(engine)

    def setUp(self):
        '''Create the temporary table'''
        Base.metadata.create_all(self.engine)

    def tearDown(self):
        '''Drop the temporary table'''
        Base.metadata.drop_all(self.engine)

    def test_good_relation(self):
        session = self.Session()
        group = Group(id=123,
                      urlname="something",
                      category_name="tech",
                      category_shortname="t",
                      city="london",
                      country="uk",
                      created=123,
                      description="something else",
                      lat=20.3,
                      lon=-3.1,
                      members=23,
                      name="something",
                      topics={"something": ["else"]},
                      category_id=12,
                      country_name="united kingdom",
                      timestamp="2018-02-17 12:12:12")

        new_group = Group(id=123,
                          urlname="something",
                          category_name="tech",
                          category_shortname="t",
                          city="london",
                          country="uk",
                          created=123,
                          description="something else",
                          lat=20.3,
                          lon=-3.1,
                          members=23,
                          name="something",
                          topics={"something": ["else"]},
                          category_id=12,
                          country_name="united kingdom",
                          timestamp="2018-02-17 12:12:12")

        member = GroupMember(group_id=123,
                             group_urlname="something",
                             member_id=1)

        new_member = GroupMember(group_id=123,
                                 group_urlname="something",
                                 member_id=1)

        # Add the group and member
        session.add(group)
        session.commit()

        session.add(member)
        session.commit()

        # Shouldn't be able to add duplicate data
        del group
        session.add(new_group)
        self.assertRaises(IntegrityError, session.commit)
        session.rollback()

        del member
        session.add(new_member)
        self.assertRaises(IntegrityError, session.commit)
        session.rollback()

        # Check that everything is in order
        self.assertEqual(session.query(Group).count(), 1)
        self.assertEqual(session.query(GroupMember).count(), 1)
        session.close()


if __name__ == "__main__":
    unittest.main()
