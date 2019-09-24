"""
Companies House ORM
"""

from sqlalchemy import Column
from sqlalchemy.dialects.mysql import VARCHAR
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Company(Base):
    __tablename__ = 'ch_companies'

    company_number = Column(VARCHAR(10), primary_key=True)
    company_name = Column(VARCHAR(200))
    URI = Column(VARCHAR(200))
    company_status = Column(VARCHAR(50))
