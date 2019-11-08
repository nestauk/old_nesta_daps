'''
Example ORM
===========
'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import VARCHAR, BIGINT, TEXT
from sqlalchemy.types import INT, DATE, BOOLEAN
from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import relationship

Base = declarative_base()


class MyTable(Base):
    __tablename__ = 'template_table'

    id = Column(VARCHAR(50), primary_key=True)
    name = Column(VARCHAR(200, collation='utf8mb4_unicode_ci'))
    domain = Column(TEXT)
    funding_rounds = Column(INT)
    funding_total_usd = Column(BIGINT)
    founded_on = Column(DATE)
    last_funding_on = Column(DATE)
    closed_on = Column(DATE)
    email = Column(VARCHAR(200, collation='utf8mb4_unicode_ci'))
    long_description = Column(TEXT(collation='utf8mb4_unicode_ci'))
    is_health = Column(BOOLEAN)
    mesh_terms = Column(TEXT)
    data = relationship('MyOtherTable')


class MyOtherTable(Base):
    __tablename__ = 'template_other_table'

    my_id = Column(VARCHAR(50), ForeignKey('template_table.id'), primary_key=True)
    data = Column(BOOLEAN)
    other_data = Column(TEXT)
