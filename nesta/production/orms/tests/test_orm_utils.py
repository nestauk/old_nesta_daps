import unittest
from unittest import mock
import pytest

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import INTEGER
from sqlalchemy import Column
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import OperationalError

from nesta.production.orms.orm_utils import get_class_by_tablename
from nesta.production.orms.orm_utils import get_mysql_engine
from nesta.production.orms.orm_utils import try_until_allowed
from nesta.production.orms.orm_utils import insert_data
from nesta.production.orms.orm_utils import load_json_from_pathstub
from nesta.production.orms.orm_utils import get_es_mapping

@pytest.fixture
def alias_lookup():
    return {
        "alias1": {
            "dataset1": "field1a",
            "dataset2": "field1b"
        },
        "alias2": {
            "dataset1": "field2a",
            "dataset2": "field2b"
        }
    }

@pytest.fixture
def mapping():
    return {
        'mappings': {
            '_doc': {
                'properties': {
                    'field1a': {'type': 'keyword'},
                    'field2a': {'type': 'text'},
                }
            }
        }        
    }
    

Base = declarative_base()
class DummyModel(Base):
    __tablename__ = 'dummy_model'

    _id = Column(INTEGER, primary_key=True)
    _another_id = Column(INTEGER, primary_key=True)
    some_field = Column(INTEGER)


class DummyFunctionWrapper:
    i = 0
    def __init__(self, exc, *args):
        self.exc = exc(*args)

    def f(self):
        if self.i < 1:
            self.i += 1
            raise self.exc

class TestOrmUtils(unittest.TestCase):
    ''''''
    
    @classmethod    
    def setUpClass(cls):
        engine = get_mysql_engine("MYSQLDBCONF", "mysqldb")
        Base.metadata.drop_all(engine)

    @classmethod    
    def tearDownClass(cls):
        engine = get_mysql_engine("MYSQLDBCONF", "mysqldb")
        Base.metadata.drop_all(engine)

    def tests_insert_and_exists(self):
        data = [{"_id": 10, "_another_id":2, 
                 "some_field": 20},
                {"_id": 10, "_another_id":2,
                 "some_field": 30},  # <--- Duplicate pkey, so should be ignored
                {"_id": 20, "_another_id":2, 
                 "some_field": 30}]
        objs = insert_data("MYSQLDBCONF", "mysqldb", "production_tests",
                           Base, DummyModel, data)
        self.assertEqual(len(objs), 2)

        objs = insert_data("MYSQLDBCONF", "mysqldb", "production_tests",
                           Base, DummyModel, data)
        self.assertEqual(len(objs), 0)


    def test_get_class_by_tablename(self):
        '''Check that the DummyModel is acquired from it's __tablename__'''
        _class = get_class_by_tablename(Base, 'dummy_model')
        self.assertEqual(_class, DummyModel)
        
    def test_get_mysql_engine(self):
        '''Test that an sqlalchemy Engine is returned'''
        engine = get_mysql_engine("MYSQLDBCONF", "mysqldb")
        self.assertEqual(type(engine), Engine)

    def test_try_until_allowed(self):
        '''Test that OperationalError leads to retrying'''
        dfw = DummyFunctionWrapper(OperationalError, None, None, None)
        try_until_allowed(dfw.f)
        
    def test_bad_try_until_allowed(self):
        '''Test that non-OperationalError lead to an exception'''        
        dfw = DummyFunctionWrapper(Exception)
        self.assertRaises(Exception, try_until_allowed, dfw.f)
    

def test_load_json_from_pathstub():
    for ds in ["nih", "crunchbase"]:
        js = load_json_from_pathstub("production/orms/",
                                     f"{ds}_es_config.json")
        assert len(js) > 0

@mock.patch("nesta.production.orms.orm_utils.load_json_from_pathstub")
def test_get_es_mapping(mocked_load_json_from_pathstub, alias_lookup, 
                        mapping):
    mocked_load_json_from_pathstub.side_effect = (mapping, 
                                                  alias_lookup)
    _mapping = get_es_mapping("dataset1", "blah")    
    alias1 = _mapping["mappings"]["_doc"]["properties"].pop("alias1")
    alias2 = _mapping["mappings"]["_doc"]["properties"].pop("alias2")
    assert mapping == _mapping
    assert alias1 == {'type': 'alias', 'path': 'field1a'}
    assert alias2 == {'type': 'alias', 'path': 'field2a'}

@mock.patch("nesta.production.orms.orm_utils.load_json_from_pathstub")
def test_get_es_mapping_bad_alias(mocked_load_json_from_pathstub, 
                                  alias_lookup, mapping):
    mocked_load_json_from_pathstub.side_effect = (mapping, 
                                                  alias_lookup)
    with pytest.raises(ValueError):
        get_es_mapping("dataset2", "blah")


