import unittest
from nesta.packages.health_data.world_exporter import get_data_urls
from nesta.packages.health_data.world_exporter import iterrows

TEST_URL = 'https://s3.eu-west-2.amazonaws.com/nesta-open-data/RePORTER_PRJABS_C_FY2018_052.zip'

class TestWorldReporter(unittest.TestCase):
    
    def test_get_data_urls(self):
        '''Test whether the website is still up and running'''
        title, hrefs = get_data_urls(1)
        self.assertGreater(len(hrefs), 0)

        
    def test_iterrows(self):
        '''Test whether the CSV data is where it should be'''
        for row in iterrows(TEST_URL):
            self.assertGreater(len(row), 0)
            break

if __name__ == "__main__":
    unittest.main()
