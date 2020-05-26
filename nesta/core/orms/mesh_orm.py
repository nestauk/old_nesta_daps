'''MeSH Schema
==============

'''


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import VARCHAR
from sqlalchemy.types import INTEGER
from sqlalchemy import Column, Table, ForeignKey

from nesta.core.orms.nih_orm import Projects

Base = declarative_base()

class MeshTerms(Base):
    __tablename__ = 'mesh_terms'

    id = Column(INTEGER, primary_key=True)
    term = Column(VARCHAR(250), primary_key=True)

class ProjectMeshTerms(Base):
    """Association table for NIH projects and their MeSH terms."""
    __tablename__ = 'nih_mesh_terms'
    project_id = Column(
            INTEGER,
            ForeignKey(Projects.application_id),
            primary_key=True,
            autoincrement=False,)
    mesh_term_id = Column(
            INTEGER,
            ForeignKey(MeshTerms.id),
            primary_key=True,
            autoincrement=False,)
