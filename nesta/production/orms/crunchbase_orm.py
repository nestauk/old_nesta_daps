'''
Crunchbase
======
'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import VARCHAR, BIGINT, TEXT, DECIMAL
from sqlalchemy.types import INT, DATE, DATETIME, FLOAT
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
    country = Column(VARCHAR(200))
    state_code = Column(VARCHAR(2))
    region = Column(VARCHAR(100))
    city = Column(VARCHAR(100))
    location_id = Column(VARCHAR(400))
    address = Column(VARCHAR(200))
    status = Column(VARCHAR(9))
    short_description = Column(VARCHAR(200))
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
    long_description = Column(TEXT)
    parent_id = Column(VARCHAR(50))
    categories = relationship('CategoryGroup',
                              secondary='crunchbase_organizations_categories')


class OrganizationCategory():
    __tablename__ = 'crunchbase_organizations_categories'

    organization_id = Column(VARCHAR(50), ForeignKey('crunchbase_organizations.id'), primary_key=True)
    category_id = Column(VARCHAR(50), ForeignKey('crunchbase_category_groups.id'), primary_key=True)


class CategoryGroup():
    __tablename__ = 'crunchbase_category_groups'

    id = Column(VARCHAR(50), primary_key=True)
    category_name = Column(VARCHAR(100))
    category_group_list = Column(VARCHAR(150))


# TODO: the below are automatically generated and need sense checking
'''
class Acquisition(Base):
    __tablename__ = 'acquisitions'

    acquiree_name = Column(VARCHAR(100))
    acquiree_country_code = Column(VARCHAR(3))
    state_code = Column(VARCHAR(2))
    acquiree_region = Column(VARCHAR(38))
    acquiree_city = Column(VARCHAR(32))
    acquirer_name = Column(VARCHAR(100))
    acquirer_country_code = Column(VARCHAR(3))
    acquirer_state_code = Column(VARCHAR(2))
    acquirer_region = Column(VARCHAR(38))
    acquirer_city = Column(VARCHAR(32))
    acquisition_type = Column(VARCHAR(17))
    acquired_on = Column(VARCHAR(10))
    price_usd = Column(FLOAT)
    price = Column(FLOAT)
    price_currency_code = Column(VARCHAR(3))
    acquiree_cb_url = Column(VARCHAR(127))
    acquirer_cb_url = Column(VARCHAR(170))
    acquiree_uuid = Column(VARCHAR(36))
    acquirer_uuid = Column(VARCHAR(36))
    acquisition_uuid = Column(VARCHAR(36))
    created_at = Column(VARCHAR(19))
    updated_at = Column(VARCHAR(19))


class Degree(Base):
    __tablename__ = 'degrees'

    degree_uuid = Column(VARCHAR(36))
    institution_uuid = Column(VARCHAR(36))
    person_uuid = Column(VARCHAR(36))
    degree_type = Column(VARCHAR(100))
    subject = Column(VARCHAR(100))
    started_on = Column(VARCHAR(10))
    completed_on = Column(VARCHAR(10))
    is_completed = Column(VARCHAR(1))
    created_at = Column(VARCHAR(19))
    updated_at = Column(VARCHAR(19))


class FundingRound(Base):
    __tablename__ = 'funding_rounds'

    company_name = Column(VARCHAR(100))
    country_code = Column(VARCHAR(3))
    state_code = Column(VARCHAR(2))
    region = Column(VARCHAR(38))
    city = Column(VARCHAR(32))
    investment_type = Column(VARCHAR(21))
    announced_on = Column(VARCHAR(10))
    raised_amount_usd = Column(FLOAT)
    raised_amount = Column(FLOAT)
    raised_amount_currency_code = Column(VARCHAR(3))
    post_money_valuation_usd = Column(FLOAT)
    post_money_valuation = Column(FLOAT)
    post_money_currency_code = Column(VARCHAR(3))
    investor_count = Column(FLOAT)
    cb_url = Column(VARCHAR(154))
    company_uuid = Column(VARCHAR(36))
    funding_round_uuid = Column(VARCHAR(36))
    created_at = Column(VARCHAR(19))
    updated_at = Column(VARCHAR(19))
    investor_names = Column(VARCHAR(192))
    investor_uuids = Column(VARCHAR(334))


class Fund(Base):
    __tablename__ = 'funds'

    entity_uuid = Column(VARCHAR(36))
    fund_uuid = Column(VARCHAR(36))
    fund_name = Column(VARCHAR(100))
    announced_on = Column(VARCHAR(10))
    raised_amount = Column(FLOAT)
    raised_amount_currency_code = Column(VARCHAR(3))
    created_at = Column(VARCHAR(19))
    updated_at = Column(VARCHAR(19))


class InvestmentPartner(Base):
    __tablename__ = 'investment_partners'

    funding_round_uuid = Column(VARCHAR(36))
    investor_uuid = Column(VARCHAR(36))
    partner_uuid = Column(VARCHAR(36))


class Investment(Base):
    __tablename__ = 'investments'

    funding_round_uuid = Column(VARCHAR(36))
    investor_uuid = Column(VARCHAR(36))
    is_lead_investor = Column(VARCHAR(1))


class Investor(Base):
    __tablename__ = 'investors'

    investor_name = Column(VARCHAR(193))
    roles = Column(VARCHAR(23))
    domain = Column(VARCHAR(80))
    country_code = Column(VARCHAR(3))
    state_code = Column(VARCHAR(2))
    region = Column(VARCHAR(2))
    city = Column(VARCHAR(32))
    investor_type = Column(VARCHAR(25))
    investment_count = Column(FLOAT)
    total_funding_usd = Column(FLOAT)
    founded_on = Column(VARCHAR(10))
    closed_on = Column(VARCHAR(10))
    cb_url = Column(VARCHAR(170))
    logo_url = Column(VARCHAR(243))
    twitter_url = Column(VARCHAR(98))
    facebook_url = Column(VARCHAR(523))
    uuid = Column(VARCHAR(36))
    updated_at = Column(VARCHAR(19))


class Ipo(Base):
    __tablename__ = 'ipos'

    name = Column(VARCHAR(65))
    country_code = Column(VARCHAR(3))
    company_state_code = Column(VARCHAR(2))
    region = Column(VARCHAR(26))
    city = Column(VARCHAR(32))
    stock_exchange_symbol = Column(VARCHAR(8))
    stock_symbol = Column(VARCHAR(12))
    went_public_on = Column(VARCHAR(10))
    price_usd = Column(FLOAT)
    price = Column(FLOAT)
    price_currency_code = Column(VARCHAR(3))
    money_raised_usd = Column(FLOAT)
    cb_url = Column(VARCHAR(109))
    ipo_uuid = Column(VARCHAR(36))
    company_uuid = Column(VARCHAR(36))
    created_at = Column(VARCHAR(19))
    updated_at = Column(VARCHAR(19))


class Job(Base):
    __tablename__ = 'jobs'

    person_uuid = Column(VARCHAR(36))
    org_uuid = Column(VARCHAR(36))
    job_uuid = Column(VARCHAR(36))
    started_on = Column(VARCHAR(10))
    ended_on = Column(VARCHAR(10))
    is_current = Column(VARCHAR(1))
    title = Column(VARCHAR(100))
    job_type = Column(VARCHAR(14))


class People(Base):
    __tablename__ = 'people'

    first_name = Column(VARCHAR(99))
    last_name = Column(VARCHAR(96))
    country_code = Column(VARCHAR(3))
    state_code = Column(VARCHAR(2))
    city = Column(VARCHAR(32))
    cb_url = Column(VARCHAR(124))
    logo_url = Column(VARCHAR(136))
    twitter_url = Column(VARCHAR(1013))
    facebook_url = Column(VARCHAR(523))
    linkedin_url = Column(VARCHAR(897))
    primary_affiliation_organization = Column(VARCHAR(100))
    primary_affiliation_title = Column(VARCHAR(100))
    primary_organization_uuid = Column(VARCHAR(36))
    gender = Column(VARCHAR(20))
    uuid = Column(VARCHAR(36))
    created_at = Column(VARCHAR(19))
    updated_at = Column(VARCHAR(19))
'''
