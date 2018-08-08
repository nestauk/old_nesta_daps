import unittest
from world_reporter import get_data
from world_reporter import get_abstract
from pyvirtualdisplay import Display

class TestWorldReporter(unittest.TestCase):

    def setUp(self):
        self.display = Display(visible=0, size=(1366, 768))
        self.display.start()

        
    def tearDown(self):
        self.display.stop()

    
    def test_get_abstract(self):
        text = get_abstract(("https://worldreport.nih.gov/app/#!/"
                             "researchOrgId=6674&programId=60569"))
        self.assertGreater(len(text), 0)

        
    def test_get_data(self):
        data = get_data()
        self.assertGreater(len(data), 0)


if __name__ == "__main__":
    unittest.main()
