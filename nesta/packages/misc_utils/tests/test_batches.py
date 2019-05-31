import pytest
from unittest import mock

from nesta.packages.misc_utils.batches import split_batches
from nesta.packages.misc_utils.batches import BatchWriter


@pytest.fixture
def generate_test_data():
    def _generate_test_data(n):
        return [{'data': 'foo', 'other': 'bar'} for i in range(n)]
    return _generate_test_data


@pytest.fixture
def generate_test_set_data():
    def _generate_test_set_data(n):
        return {f'data{i}' for i in range(n)}
    return _generate_test_set_data


def test_split_batches_when_data_is_smaller_than_batch_size(generate_test_data):
    yielded_batches = []
    for batch in split_batches(generate_test_data(200), batch_size=1000):
        yielded_batches.append(batch)

    assert len(yielded_batches) == 1


def test_split_batches_yields_multiple_batches_with_exact_fit(generate_test_data):
    yielded_batches = []
    for batch in split_batches(generate_test_data(2000), batch_size=1000):
        yielded_batches.append(batch)

    assert len(yielded_batches) == 2


def test_split_batches_yields_multiple_batches_with_remainder(generate_test_data):
    yielded_batches = []
    for batch in split_batches(generate_test_data(2400), batch_size=1000):
        yielded_batches.append(batch)

    assert len(yielded_batches) == 3


def test_split_batches_with_set(generate_test_set_data):
    yielded_batches = []
    for batch in split_batches(generate_test_set_data(2400), batch_size=1000):
        yielded_batches.append(batch)

    assert len(yielded_batches) == 3


def test_batch_writer_append_calls_function_when_limit_exceeded():
    mock_function_to_call = mock.Mock()
    batch_writer = BatchWriter(limit=4, function=mock_function_to_call)

    batch = [1, 2, 3, 4, 5]
    for b in batch:
        batch_writer.append(b)

    mock_function_to_call.assert_called_once_with([1, 2, 3, 4])


def test_batch_writer_extend_calls_while_limit_is_exceeded():
    mock_function_to_call = mock.Mock()
    batch_writer = BatchWriter(limit=3, function=mock_function_to_call)

    batches = ([1, 2], [3, 4, 5, 6, 7])
    for batch in batches:
        batch_writer.extend(batch)

    assert mock_function_to_call.mock_calls == [mock.call([1, 2, 3]),
                                                mock.call([4, 5, 6])]


def test_batch_writer_calls_function_with_args():
    mock_function_to_call = mock.Mock()
    mock_arg = mock.Mock()
    mock_kwarg = mock.Mock()
    batch_writer = BatchWriter(2, mock_function_to_call,
                               mock_arg, some_kwarg=mock_kwarg)

    batch = [1, 2]
    for b in batch:
        batch_writer.append(b)

    mock_function_to_call.assert_called_once_with([1, 2], mock_arg, some_kwarg=mock_kwarg)
