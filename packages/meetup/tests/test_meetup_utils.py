import unittest

from meetup.meetup_utils import flatten_data


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
