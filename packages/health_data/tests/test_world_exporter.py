import unittest
from world_exporter import get_csv_data
from world_exporter import get_abstract


class TestWorldReporter(unittest.TestCase):
    
    def test_get_abstract(self):
        '''Test whether the website is still up and running'''
        text = get_abstract(("https://worldreport.nih.gov/app/#!/"
                             "researchOrgId=6674&programId=60569"))
        self.assertGreater(len(text), 0)

        
    def test_get_data(self):
        '''Test whether the CSV data is where it should be'''
        data = get_csv_data()
        self.assertGreater(len(data), 0)




if __name__ == "__main__":
    unittest.main()
