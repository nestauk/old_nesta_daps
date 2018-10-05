import unittest

from nesta.packages.meetup.groups_members import get_members
from nesta.packages.meetup.groups_members import get_all_members


class TestGetMembers(unittest.TestCase):
    group_id = 22967874
    group_urlname = "A dummy name"

    def test_get_members(self):
        '''Test that the Meetup API hasn't changed fundamentally'''        
        params = dict(offset=0, page=200, group_id=TestGetMembers.group_id)
        _results = get_members(params)
        self.assertGreater(len(_results), 0)


    def test_get_all_members(self):
        '''
        Test that the Meetup API output hasn't 
        changed fundamentally
        '''
        members = get_all_members(group_id=TestGetMembers.group_id, 
                                  group_urlname=TestGetMembers.group_urlname,
                                  max_results=200, test=True)
        self.assertGreater(len(members), 0)


if __name__ == '__main__':
    unittest.main()
