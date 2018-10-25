import os
import pytest
from unittest.mock import Mock, call
from nesta.production.routines.health_data.nih_data import nih_process_task


@pytest.fixture
def environs():
    os.environ['LUIGI_CONFIG_PATH'] = 'test'
    os.environ['ESCONFIG'] = 'test'


@pytest.fixture
def process_task(environs):
    pt = nih_process_task.ProcessTask(batchable='',
                                      job_def='',
                                      job_name='',
                                      job_queue='',
                                      region_name='',
                                      db_config_path='')
    return pt


@pytest.fixture
def mocked_query():
    mocked_first = Mock()
    mocked_first.application_id = 0
    mocked_last = Mock()
    mocked_last.application_id = 100
    mocked_response = [mocked_first, mocked_last]

    query = Mock()
    query.order_by().filter().limit().all.return_value = mocked_response
    query.reset_mock()  # remove calls after the above
    return query


def test_batch_limits_calls_database_correctly(mocked_query, process_task):
    batches = process_task.batch_limits(mocked_query, 1000)
    batch = next(batches)
    expected_calls = [call.order_by(nih_process_task.Projects.application_id),
                      call.order_by().filter(nih_process_task.Projects.application_id > 0),
                      call.order_by().filter().limit(1000),
                      call.order_by().filter().limit().all()
                      ]
    assert batch[0].application_id == 0
    assert batch[-1].appliction_id == 100
    assert mocked_query.mock_calls == expected_calls


def test_batch_limits_correctly_breaks_when_all_data_returned(mocked_query, process_task):
    pass


def test_batch_limits_requests_correct_batch_size_in_test_mode():
    pass


def test_batch_limits_breaks_after_2_batches_in_test_mode():
    pass



