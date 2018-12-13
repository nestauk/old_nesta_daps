from datetime import date
import pytest

from nesta.production.routines.crunchbase.crunchbase_org_collect_task import OrgCollectTask


@pytest.fixture
def org_collect_task():
    return OrgCollectTask(date=date(year=2028, month=1, day=1), _routine_id='',
                          db_config_env='MYSQLDB', test=True)


@pytest.fixture
def gen_test_data():
    def _gen_test_data(n):
        return [{'data': 'foo', 'other': 'bar'} for i in range(n)]
    return _gen_test_data


def test_split_batches_when_data_is_smaller_than_batch_size(org_collect_task, gen_test_data):
    yielded_batches = []
    for batch in org_collect_task._split_batches(gen_test_data(200), batch_size=1000):
        yielded_batches.append(batch)

    assert len(yielded_batches) == 1


def test_split_batches_yields_multiple_batches_with_exact_fit(org_collect_task, gen_test_data):
    yielded_batches = []
    for batch in org_collect_task._split_batches(gen_test_data(2000), batch_size=1000):
        yielded_batches.append(batch)

    assert len(yielded_batches) == 2


def test_split_batches_yields_multiple_batches_with_remainder(org_collect_task, gen_test_data):
    yielded_batches = []
    for batch in org_collect_task._split_batches(gen_test_data(2400), batch_size=1000):
        yielded_batches.append(batch)

    assert len(yielded_batches) == 3


def test_total_records_returns_correct_totals(org_collect_task, gen_test_data):
    returned_data = {'inserted': gen_test_data(100),
                     'existing': gen_test_data(0),
                     'failed': gen_test_data(230)
                     }
    expected_result = {'inserted': 100,
                       'existing': 0,
                       'failed': 230,
                       'total': 330,
                       'batch_total': 330
                       }
    assert org_collect_task._total_records(returned_data) == expected_result


def test_total_records_returns_correct_totals_with_batches(org_collect_task, gen_test_data):
    returned_data = {'inserted': gen_test_data(0),
                     'existing': gen_test_data(40),
                     'failed': gen_test_data(920)
                     }
    previous_totals = {'inserted': 100,
                       'existing': 0,
                       'failed': 230,
                       'total': 330,
                       'batch_total': 100
                       }
    expected_result = {'inserted': 100,
                       'existing': 40,
                       'failed': 1150,
                       'total': 1290,
                       'batch_total': 960
                       }
    assert org_collect_task._total_records(returned_data, previous_totals) == expected_result
