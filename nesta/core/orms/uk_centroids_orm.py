'''
UK geography lookup ORM
=======================

Lookups between UK geographies and their centroids (expressed as )
'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, VARCHAR

Base = declarative_base()

class TTWAToLSOALookup(Base):
    __tablename__ = 'onsOpenGeo_ttwa_lsoa_lookup'
    ttwa_id = Column(VARCHAR(9), index=True)
    lsoa_id = Column(VARCHAR(9), primary_key=True)
    centroids = relationship(OACentroidsLookup,
                             secondary=LSOA_to_OA_Lookup)

class LSOAToOALookup(Base):
    __tablename__ = 'onsOpenGeo_lsoa_oa_lookup'
    oa_id = Column(VARCHAR(9), primary_key=True)
    lsoa_id = Column(VARCHAR(9), ForeignKey(TTWA_to_LSOA_Lookup.lsoa_id),, index=True)
    centroids = relationship(OACentroidsLookup)

class OACentroidsLookup(Base):
    __tablename__ = 'onsOpenGeo_oa_centroids_lookup'
    id = Column(VARCHAR(9), ForeignKey(LSOA_to_OA_Lookup.oa_id), primary_key=True)
    latitudes = Column(DECIMAL(2,3), index=True)
    longitudes = Column(DECIMAL(2,3), index=True)
