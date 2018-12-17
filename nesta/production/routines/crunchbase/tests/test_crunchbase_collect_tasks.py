from datetime import date
import pytest

from nesta.production.routines.crunchbase.crunchbase_org_collect_task import OrgCollectTask


@pytest.fixture
def org_collect_task():
    return OrgCollectTask(date=date(year=2028, month=1, day=1), _routine_id='',
                          db_config_env='MYSQLDB', test=True)

