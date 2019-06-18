from unittest import TestCase
from nesta.production.luigihacks.misctools import get_config
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub


class TestMiscTools(TestCase):
    def test_get_config(self):
        get_config("mysqldb.config", "mysqldb")
        with self.assertRaises(KeyError):
            get_config("mysqldb.config", "invalid")
        with self.assertRaises(KeyError):
            get_config("not_found.config", "mysqldb")

    def test_find_filepath_from_pathstub(self):
        find_filepath_from_pathstub("nesta/packages")
        with self.assertRaises(FileNotFoundError):
            find_filepath_from_pathstub("nesta/package")
