from unittest import TestCase
from nesta.production.luigihacks.misctools import get_config
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub

class TestMiscTools(TestCase):
    
    # def test_get_config(self):
    #     get_config("mysqldb.config", "mysqldb")
    #     with self.assertRaises(KeyError):
    #         get_config("mysqldb.config", "mysqld")
    #     with self.assertRaises(KeyError):
    #         get_config("mysqldb.confi", "mysqldb")
        
    def test_find_filepath_from_pathstub(self):
        find_filepath_from_pathstub("nesta/packages")
        with self.assertRaises(FileNotFoundError):
            r = find_filepath_from_pathstub("nesta/package")
            print(r)
