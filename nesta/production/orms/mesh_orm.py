'''MeSH Schema
==============

'''


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import VARCHAR
from sqlalchemy.types import INTEGER
from sqlalchemy import Column


Base = declarative_base()

class MeshTerms(Base):
    __tablename__ = 'mesh_terms'

    id = Column(INTEGER, primary_key=True)
    term = Column(VARCHAR(250), primary_key=True)

