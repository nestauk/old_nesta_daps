'''
UK geography lookup ORM
=======================

Administrative lookups between UK geographies
'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, VARCHAR

Base = declarative_base()


class UkGeographyLookup(Base):
    __tablename__ = 'onsOpenGeo_geographic_lookup'
    id = Column(VARCHAR(9), primary_key=True)
    parent_id = Column(VARCHAR(9), primary_key=True)
