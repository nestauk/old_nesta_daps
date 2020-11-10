'''
Drop non-matching abstracts
===========================

There are < 0.011% (128, out of 1.2 million, at time of writing) abstracts
with an application ID that don't match any NiH project, as they should.
Dropping them allows for other pipelines not to have to deal with
this very marginal edge case, and otherwise assume consistency
between the nih_abstracts and nih_projects tables, and
also put ForeignKey contraints on child tables.

If a matching project comes up in future, they will automatically be
picked up in future collections.
'''

import logging
import luigi
from datetime import datetime as dt

from nesta.core.luigihacks.mysqldb import make_mysql_target
from nesta.core.orms.nih_orm import Abstracts, Projects
from nesta.core.orms.nih_orm import Base
from nesta.core.orms.orm_utils import get_mysql_engine
from nesta.core.orms.orm_utils import db_session


class DropAbstracts(luigi.Task):
    '''Drop abstracts which don't match to projects. This is a tiny
    proportion of abstracts (< 0.011%). If a matching project comes up
    in future, they will automatically be picked up in future collections.
    
    Args:
        date (datetime): Date stamp.
        test (bool): Running in test mode?
    '''
    date = luigi.DateParameter()
    test = luigi.BoolParameter()

    def output(self):
        return make_mysql_target(self)

    def run(self):
        database = 'dev' if self.test else 'production'
        engine = get_mysql_engine("MYSQLDB", "mysqldb", database)
        with db_session(engine) as sess:
            # Query missing application ids
            id_ =  Abstracts.application_id
            join_ = id_ == Projects.application_id
            filter_ = Projects.application_id == None
            q = sess.query(id_,).outerjoin(Projects, join_).filter(filter_)
            # Drop the missing ids
            dud_ids, = zip(*q.all())
            delete_stmt = Abstracts.__table__.delete().where(id_.in_(dud_ids))
            sess.execute(delete_stmt)
        self.output().touch()


class RootTask(luigi.WrapperTask):    
    date = luigi.DateParameter(default=dt.now())
    production = luigi.BoolParameter(default=False)
    
    def requires(self):
        yield DropAbstracts(date=self.date, test=not self.production)
