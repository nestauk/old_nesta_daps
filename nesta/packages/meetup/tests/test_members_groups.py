import unittest

from ..members_groups import get_member_details
from ..members_groups import get_member_groups
from ..members_groups import NoMemberFound


class TestMemberGroups(unittest.TestCase):
    _id = 256456866
    _dummy_group = {'group': {'id': 1, 'urlname': "a"}}
    _info = {'id': 219332526,  
             'memberships': {'organizer': [_dummy_group],
                             'member': [_dummy_group]}}
    fake_id = 256456866000

    def test_get_member_details(self):
        '''Test that the Meetup API hasn't changed fundamentally'''        
        _results = get_member_details(TestMemberGroups._id, 200)
        self.assertGreater(len(_results), 0)
        self.assertEqual(_results['id'], TestMemberGroups._id)


    def test_get_member_groups(self):
        '''
        Test that the Meetup API output hasn't 
        changed fundamentally
        '''
        _results = get_member_groups(TestMemberGroups._info)
        self.assertGreater(len(_results), 0)


    def test_nomemberfound(self):
        '''Test that bad IDs don't return a result'''
        self.assertRaises(NoMemberFound, get_member_details, 
                          member_id=TestMemberGroups.fake_id, 
                          max_results=200)


if __name__ == '__main__':
    unittest.main()
