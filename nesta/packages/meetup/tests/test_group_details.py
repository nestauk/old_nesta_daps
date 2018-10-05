import unittest

from nesta.packages.meetup.group_details import NoGroupFound
from nesta.packages.meetup.group_details import get_group_details


class TestGroupDetails(unittest.TestCase):
    group_urlname = "Agile-Minds-GDL"
    fake_group_urlname = "123-dummy-name-123"

    def test_get_group_details(self):
        '''Test that the Meetup API hasn't changed fundamentally'''        
        _results = get_group_details(TestGroupDetails.group_urlname, 200)
        self.assertGreater(len(_results), 0)


    def test_nogroupfound(self):
        '''Test that the an error is raised if no group is found'''
        self.assertRaises(NoGroupFound, get_group_details, 
                          TestGroupDetails.fake_group_urlname, 200)


if __name__ == '__main__':
    unittest.main()
