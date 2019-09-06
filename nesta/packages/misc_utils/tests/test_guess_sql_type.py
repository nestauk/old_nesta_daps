import pytest
from nesta.packages.misc_utils.guess_sql_type import guess_sql_type

@pytest.fixture
def int_data():
    return [1,2,4,False]

@pytest.fixture
def text_data():
    return ['a', True, 2, 
            ('A very long sentence A very long sentence A '
             'very long sentence A very long sentence'), 'd']

@pytest.fixture
def float_data():
    return [1,2.3,True,None]

@pytest.fixture
def bool_data():
    return [True,False,None]


def test_guess_sql_type_int(int_data):
    assert guess_sql_type(int_data) == 'INTEGER'

def test_guess_sql_type_float(float_data):
    assert guess_sql_type(float_data) == 'FLOAT'

def test_guess_sql_type_bool(bool_data):
    assert guess_sql_type(bool_data) == 'BOOLEAN'

def test_guess_sql_type_str(text_data):
    assert guess_sql_type(text_data, text_len=10) == 'TEXT'
    assert guess_sql_type(text_data, text_len=100).startswith('VARCHAR(')
