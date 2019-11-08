import pytest
from nesta.packages.misc_utils.camel_to_snake import camel_to_snake

@pytest.fixture
def lower_camel():
    return 'camelCase'

@pytest.fixture
def upper_camel():
    return 'CamelCase'

@pytest.fixture
def snake_camel():
    return 'camel_case'

def test_lower_camel(lower_camel, snake_camel):
    assert camel_to_snake(lower_camel) == snake_camel

def test_upper_camel(upper_camel, snake_camel):
    assert camel_to_snake(upper_camel) == snake_camel

def test_snake_camel(snake_camel):
    assert camel_to_snake(snake_camel) == snake_camel
