import unittest
import os
from pycountry import countries as pycountries
import pandas as pd

from packages.meetup.country_groups import MeetupCountryGroups
from packages.meetup.country_groups import get_coordinate_data
from packages.meetup.country_groups import assert_iso2_key


class TestGeoCoding(unittest.TestCase):
    def test_country_lookup_generally_works(self):
        '''Test that the list of countries in WORLD BORDERS 
        and pycountries are roughly consistent'''

        # Stimulate pycountries into action
        pycountries.get(name="Mexico")

        # Load the shapefile data
        df = get_coordinate_data(2)

        # Count the number of passes
        passed = 0
        for country in pycountries.objects:
            try:
                assert_iso2_key(df, country.alpha_2)
            except KeyError:
                pass
            else:
                passed += 1

        # Fully expect some not to pass, but most to pass
        self.assertNotEqual(passed, 0)
        self.assertGreater(passed/len(df), 0.98)


class TestMeetupApiCountryGroups(unittest.TestCase):
    def test_meetup_api(self):
        '''Test that the Meetup API hasn't changed drastically'''
        mcg = MeetupCountryGroups(iso2="MX", category=34)
        mcg.get_groups(lat=23.63, lon=-102.55, max_pages=3)
        self.assertGreater(len(mcg.groups), 0)


if __name__ == '__main__':
    unittest.main()
