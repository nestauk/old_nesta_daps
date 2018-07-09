import unittest
import os

from meetup.country_groups import MeetupCountryGroups
from pycountry import countries as pycountries
from meetup.country_groups import flatten_data


class TestGeoCoding(unittest.TestCase):
    def test_all_countries_in_pycountry(self):
        '''Test that the list of countries is still valid'''
        file_path_plus_name = os.path.abspath(__file__)
        file_path = os.path.dirname(file_path_plus_name)
        file_path_csv = os.path.join(file_path, "countries.csv")
        with open(file_path_csv) as f:
            countries = [line for line in f.read().split("\n")]
        # Prompt pycountry to load up its database
        pycountries.get(name="Mexico")
        self.assertEqual(len(pycountries.objects), len(countries))
        try:
            for c in countries:
                pycountries.get(name=c) 
        except KeyError:
            self.fail("Country %s not found in pycountries. Update the dataset.")


class TestMeetupApiCountryGroups(unittest.TestCase):
    def test_googlev3_api(self):
        '''Test that the GoogleV3 API hasn't changed drastically'''
        try:
            mcg = MeetupCountryGroups(country="Mexico", category=34)
        except Exception:  # Replace this with the actual exception
            self.fail("The GoogleV3 API has failed.")


    def test_meetup_api(self):
        '''Test that the Meetup API hasn't changed drastically'''
        mcg = MeetupCountryGroups(country="Mexico", category=34, 
                                  radius=300, lat=23.63, lon=-102.55,
                                  page=200)
        mcg.get_groups(max_pages=3)
        self.assertTrue(len(mcg.groups) > 0)
        
        # Test the flattener
        flattened = flatten_data(mcg, desired_keys=[('category', 'name'),
                                                    ('category', 'shortname'),
                                                    ('category', 'id'),
                                                    'description',
                                                    'created',
                                                    'country',
                                                    'city',
                                                    'id',
                                                    'lat',
                                                    'lon',
                                                    'members',
                                                    'name',
                                                    'topics'])
        self.assertTrue(len(flattened) > 0)


if __name__ == '__main__':
    unittest.main()
