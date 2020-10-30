"""
arXiv vectors
==============

Tasks for converting arXiv abstracts to vectors via BERT in batches.
"""

from nesta.core.luigihacks.luigi_logging import set_log_level
from nesta.core.luigihacks.parameter import SqlAlchemyParameter
from nesta.core.luigihacks.misctools import f3p
from nesta.core.luigihacks.misctools import load_batch_config
from nesta.core.luigihacks.text2vectask import Text2VecTask
from nesta.core.orms.nih_orm import Projects, PhrVector
from nesta.core.orms.nih_orm import Abstracts, AbstractVector

import luigi
from datetime import datetime as dt


TASK_KWARGS = [
    (Abstracts, Abstracts.application_id, Abstracts.abstract_text, AbstractVector),
    (Projects, Projects.application_id, Projects.phr, PhrVector),
]

class RootTask(luigi.WrapperTask):
    process_batch_size = luigi.IntParameter(default=10000)
    production = luigi.BoolParameter(default=False)
    date = luigi.DateParameter(default=dt.now())

    def requires(self):
        set_log_level(not self.production)
        batch_kwargs = load_batch_config(self, memory=16000, 
                                         vcpus=4, max_live_jobs=10,
                                         date=self.date,
                                         job_def="py37_amzn2_pytorch")
        for in_class, id_field, text_field, out_class in TASK_KWARGS:
            in_class_name = in_class.__tablename__
            _Task = type(f'{in_class_name}-Text2VecTask-{self.date}', 
                         (Text2VecTask,), {})
            yield _Task(in_class=in_class, id_field=id_field,
                        text_field=text_field, out_class=out_class,
                        **batch_kwargs)
