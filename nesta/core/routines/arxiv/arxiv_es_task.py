'''
Arxiv data to elasticsearch
================================

Luigi routine to load the Arxiv data from MYSQL into Elasticsearch.
'''

import logging
import luigi
import datetime
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub as f3p
from nesta.core.luigihacks.sql2estask import Sql2EsTask
from nesta.core.routines.arxiv.arxiv_grid_task import GridTask
from nesta.core.luigihacks.parameter import DictParameterPlus


class ArxivESTask(Sql2EsTask):
    date = luigi.DateParameter(default=datetime.datetime.today())
    drop_and_recreate = luigi.BoolParameter(default=False)
    grid_task_kwargs = DictParameterPlus()

    def requires(self):
        yield GridTask(**self.grid_task_kwargs)
