'''
Crunchbase
======
'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import VARCHAR, BIGINT, TEXT, DECIMAL
from sqlalchemy.types import INT, DATE, DATETIME
from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import relationship

Base = declarative_base()


class Organization(Base):
    __tablename__ = 'crunchbase_organizations'

    id = Column(VARCHAR(50), primary_key=True)
    company_name = Column(VARCHAR(150))
    roles = Column(VARCHAR(50))
    permalink = Column(VARCHAR(200))
    domain = Column(TEXT)
    homepage_url = Column(TEXT)
    country_code = Column(VARCHAR(3))
    state_code = Column(VARCHAR(2))
    region = Column(VARCHAR(100))
    city = Column(VARCHAR(100))
    address = Column(VARCHAR(200))
    status = Column(VARCHAR(9))
    short_description = Column(VARCHAR(200))
    category_list = Column(TEXT)
    category_group_list = Column(TEXT)
    funding_rounds = Column(INT)
    funding_total_usd = Column(BIGINT)
    founded_on = Column(DATE)
    last_funding_on = Column(DATE)
    closed_on = Column(DATE)
    employee_count = Column(VARCHAR(20))
    email = Column(VARCHAR(200))
    phone = Column(TEXT)
    facebook_url = Column(TEXT)
    linkedin_url = Column(TEXT)
    cb_url = Column(TEXT)
    logo_url = Column(TEXT)
    twitter_url = Column(TEXT)
    aliases = Column(TEXT)
    created_at = Column(DATETIME)
    updated_at = Column(DATETIME)
    primary_role = Column(VARCHAR(50))
    type = Column(VARCHAR(50))
