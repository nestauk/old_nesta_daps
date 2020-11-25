"""
General Curate
==============

Tasks for curating and merging MySQL data, and then piping to unified table.
"""

from nesta.core.luigihacks.luigi_logging import set_log_level
from nesta.core.luigihacks.sql2batchtask import Sql2BatchTask
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p
from nesta.core.orms.crunchbase_orm import Organization as CrunchbaseOrg
from nesta.core.orms.nih_orm import Projects as NihProject
from nesta.core.orms.orm_utils import get_base_from_orm_name
from nesta.core.orms.orm_utils import get_class_by_tablename
from nesta.core.luigihacks.misctools import get_config
from nesta.core.luigihacks.mysqldb import make_mysql_target


from sqlalchemy.sql import text as sql_text
import luigi
from datetime import datetime as dt
import os
import pathlib
import yaml
from functools import lru_cache


S3_BUCKET='nesta-production-intermediate'
ENV_FILES = ['mysqldb.config', 'nesta']


@lru_cache()
def read_config():
    """Read raw data from the config file"""
    this_path = pathlib.Path(__file__).parent.absolute()
    with open(this_path / 'curate_config.yaml') as f:
        return yaml.safe_load(f)


def get_datasets():
    """Return unique values of the 'dataset' from the config"""
    return list(set(item['dataset'] for item in read_config()))


def parse_config():
    """Yield this task's parameter fields from the config"""
    config = read_config()
    for item in config:
        # Required fields
        dataset = item['dataset']
        orm = item['orm']
        table_name = item['table_name']
        _id_field = item['id_field']

        # Optional fields
        filter = item.get('filter', None)
        if filter is not None:
            filter = sql_text(filter)
        extra_kwargs = item.get('batchable_kwargs', {})

        # Extract the actual ORM ID field
        Base = get_base_from_orm_name(orm)
        _class = get_class_by_tablename(Base, table_name)
        id_field = getattr(_class, _id_field)

        # Yield this curation task's parameters
        yield dataset, id_field, filter, extra_kwargs


def kwarg_maker(dataset, routine_id):
    """kwarg factory for Sql2BatchTask tasks"""
    return dict(routine_id=f'{routine_id}_{dataset}',
                env_files=[f3p(f) for f in ENV_FILES],
                batchable=f3p(f'batchables/general/{dataset}/curate'))


class CurateTask(luigi.Task):
    process_batch_size = luigi.IntParameter(default=1000)
    production = luigi.BoolParameter(default=False)
    date = luigi.DateParameter(default=dt.now())
    dataset = luigi.ChoiceParameter(default='all',
                                    choices=['all'] + get_datasets())

    def output(self):
        return make_mysql_target(self)

    def requires(self):
        set_log_level(True)
        routine_id = f'General-Curate-{self.date}'
        default_kwargs = dict(date=self.date,
                              process_batch_size=self.process_batch_size,
                              job_def='py37_amzn2',
                              job_name=routine_id,
                              job_queue='HighPriority',
                              region_name='eu-west-2',
                              poll_time=10,
                              max_live_jobs=50,
                              db_config_env='MYSQLDB',
                              test=not self.production,
                              memory=2048,
                              intermediate_bucket=S3_BUCKET)

        # Iterate over each task specified in the config
        for dataset, id_field, filter, kwargs in parse_config():
            if self.dataset != 'all' and dataset != self.dataset:
                continue
            yield Sql2BatchTask(id_field=id_field,
                                filter=filter,
                                kwargs=kwargs,
                                **kwarg_maker(dataset, routine_id),
                                **default_kwargs)

    def run(self):
        self.output().touch()
