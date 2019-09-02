import pytest

from nesta.core.luigihacks.misctools import get_config
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub


def test_get_config():
    get_config("mysqldb.config", "mysqldb")
    with pytest.raises(KeyError):
        get_config("mysqldb.config", "invalid")
    with pytest.raises(KeyError):
        get_config("not_found.config", "mysqldb")


def test_find_filepath_from_pathstub():
    find_filepath_from_pathstub("nesta/packages")
    with pytest.raises(FileNotFoundError):
        find_filepath_from_pathstub("nesta/package")
