'''MeSH Schema
==============

'''


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import VARCHAR
from sqlalchemy.types import INTEGER
from sqlalchemy import Column, Table, ForeignKey

from nesta.production.orms.nih_orm import Projects

Base = declarative_base()

"""Association table for NIH projects and their MeSH terms."""
project_mesh_terms = Table('nih_mesh_terms', Base.metadata,
        Column('project_id',
            INTEGER,
            ForeignKey(Projects.application_id),
            primary_key=True),
        Column('mesh_term_id',
            INTEGER,
            ForeignKey('mesh_terms.id'),
            primary_key=True)
        )

class MeshTerms(Base):
    __tablename__ = 'mesh_terms'

    id = Column(INTEGER, primary_key=True)
    term = Column(VARCHAR(250), primary_key=True)

