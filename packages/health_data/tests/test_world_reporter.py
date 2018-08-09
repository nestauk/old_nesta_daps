import unittest
from world_reporter import get_csv_data
from world_reporter import get_abstract
from pyvirtualdisplay import Display

class TestWorldReporter(unittest.TestCase):
    
    def setUp(self):
        '''Selenium won't work unless a display is hanging about'''
        self.display = Display(visible=0, size=(1366, 768))
        self.display.start()

        
    def tearDown(self):
        self.display.stop()

    
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
