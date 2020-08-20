'''
Global Research Identifier Database (GRID)
==========================================
'''
from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR, DECIMAL
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import INTEGER

from nesta.core.orms.crunchbase_orm import fixture as cb_fixture

Base = declarative_base()


class Institute(Base):
    """Institutes."""
    __tablename__ = 'grid_institutes'

    id = Column(VARCHAR(20), primary_key=True, autoincrement=False)
    name = Column(VARCHAR(250, collation='utf8_bin'))
    address_line_1 = Column(VARCHAR(50, collation='utf8_bin'))
    address_line_2 = Column(VARCHAR(50, collation='utf8_bin'))
    address_line_3 = Column(VARCHAR(50, collation='utf8_bin'))
    postcode = Column(VARCHAR(30))
    city = Column(VARCHAR(40, collation='utf8_bin'))
    geonames_city_id = Column(INTEGER)
    state = Column(VARCHAR(40))
    state_code = Column(VARCHAR(10))
    country = Column(VARCHAR(40))
    country_code = Column(VARCHAR(3))
    latitude = Column(DECIMAL(precision=8, scale=6))
    longitude = Column(DECIMAL(precision=9, scale=6))
    _aliases = relationship("Alias")
    aliases = association_proxy('_aliases', 'alias')  # direct access to the alias field


class Alias(Base):
    """Aliases."""
    __tablename__ = 'grid_aliases'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    grid_id = Column(VARCHAR(20), ForeignKey('grid_institutes.id'))
    alias = Column(VARCHAR(250, collation='utf8_bin'))


class GridCrunchbaseLookup(Base):
    __tablename__ = 'grid_crunchbase_lookup'
    grid_id = Column(VARCHAR(20), ForeignKey('grid_institutes.id'),
                     primary_key=True)
    cb_id = Column(cb_fixture('id_pk').type, 
                   ForeignKey('crunchbase_organizations.id'),
                   primary_key=True)
