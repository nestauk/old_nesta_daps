import pytest
from sqlalchemy.orm.exc import NoResultFound
from unittest import mock

from nesta.production.luigihacks.batchgeocode import GeocodeBatchTask


@pytest.fixture
def geo_batch_task():
    class MyTask(GeocodeBatchTask):
        """Empty subclass with the minimum methods to allow instantation."""
        def combine(self):
            pass

    return MyTask(job_def='', job_name='', job_queue='', region_name='', city_col='',
                  country_col='', composite_key_col='', database_config='', database='')


@mock.patch('nesta.production.luigihacks.batchgeocode.insert_data')
@mock.patch('nesta.production.luigihacks.batchgeocode.db_session')
def test_insert_new_locations(mocked_db_session, mocked_insert_data, geo_batch_task):
    geo_batch_task.engine = ''
    mocked_session = mock.Mock()
    mocked_db_session().__enter__.return_value = mocked_session
    mocked_query = mock.Mock()

    mocked_session.query.side_effect = [
                            [('London', 'United Kingdom', 'london-united_kingdom'),
                             ('Berlin', 'Germany', 'berlin-germany'),
                             ('Paris', 'France', 'paris-france'),
                             ], mocked_query, mocked_query, mocked_query
                             ]

    mocked_query.filter().one.side_effect = ['london-united_kingdom',
                                             NoResultFound,
                                             NoResultFound
                                             ]

    expected_calls = [{'id': 'berlin-germany', 'city': 'Berlin', 'country': 'Germany'},
                      {'id': 'paris-france', 'city': 'Paris', 'country': 'France'}
                      ]

    geo_batch_task._insert_new_locations()
    assert mocked_insert_data.mock_calls[0][1][5] == expected_calls


@mock.patch('nesta.production.luigihacks.batchgeocode.insert_data')
@mock.patch('nesta.production.luigihacks.batchgeocode.db_session')
def test_insert_no_new_locations(mocked_db_session, mocked_insert_data, geo_batch_task):
    geo_batch_task.engine = ''
    mocked_session = mock.Mock()
    mocked_db_session().__enter__.return_value = mocked_session
    mocked_query = mock.Mock()

    mocked_session.query.side_effect = [
                            [('London', 'United Kingdom', 'london-united_kingdom'),
                             ('Berlin', 'Germany', 'berlin-germany'),
                             ('Paris', 'France', 'paris-france'),
                             ], mocked_query, mocked_query, mocked_query
                             ]

    mocked_query.filter().one.side_effect = ['london-united_kingdom',
                                             'berlin-germany',
                                             'paris-france'
                                             ]

    geo_batch_task._insert_new_locations()
    mocked_insert_data.assert_not_called()


@mock.patch('nesta.production.luigihacks.batchgeocode.time.time')
def test_put_batch_filename(mocked_time, geo_batch_task):
    geo_batch_task.s3 = mock.Mock()
    mocked_time.return_value = 123456.7890123
    assert geo_batch_task._put_batch('some data') == 'geocoding_batch_1234567890123.json'


def test_create_batches_in_production_mode(geo_batch_task):
    # patching and setup
    geo_batch_task._put_batch = mock.Mock()
    geo_batch_task.batch_size = 1000
    geo_batch_task.test = False

    # create locations
    uncoded_location = mock.Mock()
    uncoded_location.id = 'comp_id'
    uncoded_location.city = 'some city'
    uncoded_location.country = 'some country'
    uncoded_locations = [uncoded_location for a in range(2500)]

    batches = []
    for batch in geo_batch_task._create_batches(uncoded_locations):
        batches.append(batch)

    assert len(batches) == 3


def test_create_batches_in_test_mode(geo_batch_task):
    # patching and setup
    geo_batch_task._put_batch = mock.Mock()
    geo_batch_task.batch_size = 1000
    geo_batch_task.test = True

    # create locations
    uncoded_location = mock.Mock()
    uncoded_location.id = 'comp_id'
    uncoded_location.city = 'some city'
    uncoded_location.country = 'some country'
    uncoded_locations = [uncoded_location for a in range(540)]

    batches = []
    for batch in geo_batch_task._create_batches(uncoded_locations):
        batches.append(batch)

    assert len(batches) == 11
