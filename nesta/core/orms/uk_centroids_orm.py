'''
UK geography lookup ORM
=======================

Lookups between UK geographies and their centroids (expressed as )
'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR
from sqlalchemy.types import DECIMAL
from sqlalchemy.orm import relationship

Base = declarative_base()

class LSOAToOALookup(Base):
    __tablename__ = 'onsOpenGeo_lsoa_oa_lookup'
    oa_id = Column(VARCHAR(9), primary_key=True)
    #lsoa_id = Column(VARCHAR(9), ForeignKey(TTWAToLSOALookup.lsoa_id), index=True)
    #centroids = relationship(OACentroidsLookup)


class OACentroidsLookup(Base):
    __tablename__ = 'onsOpenGeo_oa_centroids_lookup'
    id = Column(VARCHAR(9), ForeignKey(LSOAToOALookup.oa_id), primary_key=True)
    latitudes = Column(DECIMAL(9,6), index=True)
    longitudes = Column(DECIMAL(9,6), index=True)

# add attribute that reference OACentroidsLookup
LSOAToOALookup.centroids = relationship(OACentroidsLookup)


class TTWAToLSOALookup(Base):
    __tablename__ = 'onsOpenGeo_ttwa_lsoa_lookup'
    ttwa_id = Column(VARCHAR(9), index=True)
    lsoa_id = Column(VARCHAR(9), primary_key=True)
    centroids = relationship(OACentroidsLookup,
                             secondary=LSOAToOALookup)

# add attribute that reference TTWAToLSOALookup
LSOAToOALookup.lsoa_id = Column(VARCHAR(9), ForeignKey(TTWAToLSOALookup.lsoa_id), index=True)
