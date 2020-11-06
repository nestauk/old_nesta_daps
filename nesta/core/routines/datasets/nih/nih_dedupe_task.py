'''
Deduplication of near duplicates
================================

Discover near-duplicates of projects from similarity 
of document vectors, and fill out the duplicates link table.
'''

import logging
import luigi
from datetime import datetime as dt

from nesta.core.luigihacks.misctools import f3p
from nesta.core.luigihacks.mysqldb import make_mysql_target
from nesta.core.luigihacks.parameter import SqlAlchemyParameter
from nesta.core.orms.nih_orm import AbstractVector
from nesta.core.orms.nih_orm import PhrVector
from nesta.core.orms.nih_orm import TextDuplicate
from nesta.core.orms.nih_orm import Base
from nesta.core.orms.orm_utils import insert_data

from nesta.packages.vectors.similarity import generate_duplicate_links
import faiss


class VectorDedupeTask(luigi.Task):
    '''Generic task for generating a link table duplicate text fields
    
    Args:
        date (datetime): Date stamp.
        test (bool): Running in test mode?
        orm (SqlAlchemy ORM): A SqlAlchemy ORM containing the vector field.
        id_field (str): Name of the ID field in the ORM.
    '''
    date = luigi.DateParameter()
    test = luigi.BoolParameter()
    vector_orm = SqlAlchemyParameter()
    source = luigi.ChoiceParameter(choices=["phr", "abstract"],
                                   var_type=str)
    
    def output(self):
        return make_mysql_target(self)

    def run(self):
        database = 'dev' if self.test else 'production'
        max_chunks = 1 if self.test else None
        thresh = 0 if self.test else 0.5
        links = generate_duplicate_links(orm=self.vector_orm,
                                         id_field="application_id",
                                         database=database,
                                         metric=faiss.METRIC_L1,
                                         k=20,
                                         k_large=1000,
                                         n_clusters=250,
                                         duplicate_threshold=thresh,
                                         read_max_chunks=max_chunks)
        links = [{"text_field":self.source, **link} for link in links]
        insert_data("MYSQLDB", "mysqldb", database, Base,
                    TextDuplicate, links, low_memory=True)
        self.output().touch()


class RootTask(luigi.WrapperTask):    
    date = luigi.DateParameter(default=dt.now())
    production = luigi.BoolParameter(default=False)
    
    def requires(self):
        for source, orm in (("phr", PhrVector),
                            ("abstract", AbstractVector)):
            yield VectorDedupeTask(vector_orm=orm,
                                   source=source,
                                   date=self.date,
                                   test=not self.production)
            if not self.production:
                break
