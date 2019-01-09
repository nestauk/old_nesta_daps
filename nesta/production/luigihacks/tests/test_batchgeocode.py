import pytest
from sqlalchemy.orm.exc import NoResultFound
from unittest import mock

from nesta.production.luigihacks.batchgeocode import GeocodeBatchTask


@pytest.fixture
def geo_batch_task():
    class MyTask(GeocodeBatchTask):
        """Empty subclass with the minimum methods to allow instantiation."""
        database = 'the test database'

        def combine(self):
            pass

    return MyTask(job_def='', job_name='', job_queue='', region_name='', city_col='',
                  country_col='', location_key_col='', db_config_env='', test=True)


@mock.patch('nesta.production.luigihacks.batchgeocode.insert_data')
@mock.patch('nesta.production.luigihacks.batchgeocode.db_session')
def test_insert_new_locations(mocked_db_session, mocked_insert_data, geo_batch_task):
    geo_batch_task.engine = ''
    mocked_session = mock.Mock()
    mocked_db_session().__enter__.return_value = mocked_session
    mocked_query = mock.Mock()

    mocked_session.query.return_value = mocked_query
    mocked_session.query().distinct().limit.return_value = [
                            ('London', 'United Kingdom', 'london-united_kingdom'),
                            ('Berlin', 'Germany', 'berlin-germany'),
                            ('Paris', 'France', 'paris-france')
                            ]

    mocked_query.all.return_value = [('london-united_kingdom',), ('some-other_place',)]

    expected_calls = [{'id': 'berlin-germany', 'city': 'Berlin', 'country': 'Germany'},
                      {'id': 'paris-france', 'city': 'Paris', 'country': 'France'}]

    geo_batch_task._insert_new_locations()
    assert mocked_insert_data.mock_calls[0][1][5] == expected_calls


@mock.patch('nesta.production.luigihacks.batchgeocode.insert_data')
@mock.patch('nesta.production.luigihacks.batchgeocode.db_session')
def test_insert_no_new_locations(mocked_db_session, mocked_insert_data, geo_batch_task):
    geo_batch_task.engine = ''
    mocked_session = mock.Mock()
    mocked_db_session().__enter__.return_value = mocked_session
    mocked_query = mock.Mock()

    mocked_session.query.return_value = mocked_query
    mocked_session.query().distinct().limit.return_value = [
                            ('London', 'United Kingdom', 'london-united_kingdom'),
                            ('Berlin', 'Germany', 'berlin-germany'),
                            ('Paris', 'France', 'paris-france')
                            ]

    mocked_query.all.return_value = [('london-united_kingdom',),
                                     ('berlin-germany',),
                                     ('paris-france',)]

    geo_batch_task._insert_new_locations()
    mocked_insert_data.assert_not_called()


@mock.patch('nesta.production.luigihacks.batchgeocode.insert_data')
@mock.patch('nesta.production.luigihacks.batchgeocode.db_session')
def test_insert_no_duplicate_locations(mocked_db_session, mocked_insert_data, geo_batch_task):
    geo_batch_task.engine = ''
    mocked_session = mock.Mock()
    mocked_db_session().__enter__.return_value = mocked_session
    mocked_query = mock.Mock()

    mocked_session.query.return_value = mocked_query
    mocked_session.query().distinct().limit.return_value = [
                            ('London', 'United Kingdom', 'london-united_kingdom'),
                            ('Berlin', 'Germany', 'berlin-germany'),
                            ('Berlin', 'Germany', 'berlin-germany'),
                            ('Paris', 'France', 'paris-france'),
                            ('Paris', 'France', 'paris-france')
                            ]

    mocked_query.all.return_value = [('london-united_kingdom',)]

    expected_calls = [{'id': 'berlin-germany', 'city': 'Berlin', 'country': 'Germany'},
                      {'id': 'paris-france', 'city': 'Paris', 'country': 'France'}]

    geo_batch_task._insert_new_locations()
    assert mocked_insert_data.mock_calls[0][1][5] == expected_calls


@mock.patch('nesta.production.luigihacks.batchgeocode.time.time')
def test_put_batch_filename(mocked_time, geo_batch_task):
    geo_batch_task.s3 = mock.Mock()
    mocked_time.return_value = 123456.7890123
    assert geo_batch_task._put_batch('some data') == 'geocoding_batch_1234567890123.json'


@pytest.fixture
def create_uncoded_locations():
    def _create_uncoded_locations(num):
        uncoded_location = mock.Mock()
        uncoded_location.id = 'comp_id'
        uncoded_location.city = 'some city'
        uncoded_location.country = 'some country'
        return [uncoded_location for a in range(num)]
    return _create_uncoded_locations


def test_create_batches_in_production_mode(create_uncoded_locations, geo_batch_task):
    # patching and setup
    geo_batch_task._put_batch = mock.Mock()
    geo_batch_task.batch_size = 1000
    geo_batch_task.test = False

    uncoded_locations = create_uncoded_locations(2500)
    batches = []
    for batch in geo_batch_task._create_batches(uncoded_locations):
        batches.append(batch)

    assert len(batches) == 3  # 2500 records with batch size of 1000 = 3 batches


def test_create_batches_in_test_mode(create_uncoded_locations, geo_batch_task):
    # patching and setup
    geo_batch_task._put_batch = mock.Mock()
    geo_batch_task.batch_size = 1000
    geo_batch_task.test = True

    uncoded_locations = create_uncoded_locations(540)
    batches = []
    for batch in geo_batch_task._create_batches(uncoded_locations):
        batches.append(batch)

    assert len(batches) == 11  # test mode has batch size 50, so 540/50 = 11 batches


def test_create_batches_with_no_remainder(create_uncoded_locations, geo_batch_task):
    # patching and setup
    geo_batch_task._put_batch = mock.Mock()
    geo_batch_task.batch_size = 100
    geo_batch_task.test = False

    uncoded_locations = create_uncoded_locations(200)
    batches = []
    for batch in geo_batch_task._create_batches(uncoded_locations):
        batches.append(batch)

    assert len(batches) == 2  # 200 locations with batch size 100 = exactly 2 batches


@mock.patch('nesta.production.luigihacks.batchgeocode.boto3')
@mock.patch('nesta.production.luigihacks.batchgeocode.try_until_allowed')
@mock.patch('nesta.production.luigihacks.batchgeocode.get_mysql_engine')
def test_prepare_step_with_new_locations(mocked_sql_engine, mocked_try, mocked_boto,
                                         create_uncoded_locations, geo_batch_task):
    # patching and setup
    geo_batch_task._insert_new_locations = mock.Mock()
    geo_batch_task._get_uncoded = mock.Mock(return_value=['uncoded1', 'uncoded2'])
    geo_batch_task._create_batches = mock.Mock(return_value=['bat1', 'bat2', 'bat3'])

    job_params = geo_batch_task.prepare()
    assert len(job_params) == 3
    assert {'batch_file', 'db_name', 'bucket', 'config'}.issubset(job_params[0])


@mock.patch('nesta.production.luigihacks.batchgeocode.boto3')
@mock.patch('nesta.production.luigihacks.batchgeocode.try_until_allowed')
@mock.patch('nesta.production.luigihacks.batchgeocode.get_mysql_engine')
def test_prepare_step_with_no_new_locations(mocked_sql_engine, mocked_try, mocked_boto,
                                            create_uncoded_locations, geo_batch_task):
    # patching and setup
    geo_batch_task._insert_new_locations = mock.Mock()
    geo_batch_task._get_uncoded = mock.Mock(return_value=[])
    geo_batch_task._create_batches = mock.Mock()

    job_params = geo_batch_task.prepare()
    assert job_params == []
    geo_batch_task._create_batches.assert_not_called()
