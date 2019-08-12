from collections import namedtuple
import pytest


from nesta.packages.examples.example_package import some_func


@pytest.fixture
def mocked_row():
    def _mocked_row(*, id, name):
        Row = namedtuple('Row', ['id', 'name'])
        return Row(id=id, name=name)
    return _mocked_row


class TestSomeFunc:
    def test_some_func_returns_true_when_start_string_in_name(self, mocked_row):
        mocked_row = mocked_row(id=1, name='cat')
        assert some_func('cat', mocked_row) == {'my_id': 1, 'data': True}

    def test_some_func_returns_false_when_start_string_not_in_name(self, mocked_row):
        mocked_row = mocked_row(id=2, name='cat')
        assert some_func('dog', mocked_row) == {'my_id': 2, 'data': False}

    def test_some_func_returns_false_when_name_is_none(self, mocked_row):
        mocked_row = mocked_row(id=3, name=None)
        assert some_func('cat', mocked_row) == {'my_id': 3, 'data': False}
