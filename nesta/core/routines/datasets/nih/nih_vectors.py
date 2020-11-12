"""
NiH vectors
==============

Tasks for converting NiH text fields (PHR and abstracts) 
to vectors via BERT in batches.
"""

from nesta.core.luigihacks.luigi_logging import set_log_level
from nesta.core.luigihacks.parameter import SqlAlchemyParameter
from nesta.core.luigihacks.misctools import f3p
from nesta.core.luigihacks.misctools import load_batch_config
from nesta.core.luigihacks.text2vectask import Text2VecTask
from nesta.core.orms.nih_orm import Projects, PhrVector
from nesta.core.orms.nih_orm import Abstracts, AbstractVector
from nesta.core.orms.orm_utils import get_mysql_engine, db_session

import luigi
from datetime import datetime as dt
import logging
from functools import lru_cache

TASK_KWARGS = [
    (Abstracts, Abstracts.application_id, Abstracts.abstract_text, AbstractVector),
    (Projects, Projects.application_id, Projects.phr, PhrVector),
]


# Cached because luigi calls "requires" several times
# in order to resolve the DAG, and this a slow transaction
@lru_cache()
def get_done_ids(out_class, id_field, test):
    """Get ID values for the given class and ID field"""
    # MySQL setup
    database = 'dev' if test else 'production'
    engine = get_mysql_engine("MYSQLDB", "mysqldb", database)

    # Get set of all objects IDs from the database
    with db_session(engine) as session:
        vec_id_field = getattr(out_class, id_field.key)
        try:
            done_ids, = zip(*session.query(vec_id_field).all())
        except ValueError:  # Forgiveness, if there are no done IDs
            done_ids = []
    logging.info(f"Already collected {len(done_ids)} ids")
    return done_ids
    

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
            # Generate a dummy task name so we can differentiate
            # the two tasks in the luigi task visualiser
            in_class_name = in_class.__tablename__
            _Task = type(f'{in_class_name}-Text2VecTask-{self.date}', 
                         (Text2VecTask,), {})
            done_ids = get_done_ids(out_class, id_field, 
                                    not self.production)
            text_not_null = (text_field != None)
            yield _Task(in_class=in_class, id_field=id_field,
                        text_field=text_field, out_class=out_class,
                        filter_ids=done_ids, 
                        filter=text_not_null, # Ignore null text fields
                        **batch_kwargs)
