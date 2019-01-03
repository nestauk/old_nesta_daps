'''
Crunchbase
======
'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import VARCHAR, BIGINT, TEXT
from sqlalchemy.types import INT, DATE, DATETIME, BOOLEAN
from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import relationship

Base = declarative_base()


class Organization(Base):
    __tablename__ = 'crunchbase_organizations'

    id = Column(VARCHAR(50), primary_key=True)
    company_name = Column(VARCHAR(200))
    roles = Column(VARCHAR(50))
    permalink = Column(VARCHAR(200))
    domain = Column(TEXT)
    homepage_url = Column(TEXT)
    location_id = Column(VARCHAR(400), index=True)
    country = Column(VARCHAR(200))
    state_code = Column(VARCHAR(2))
    region = Column(VARCHAR(100))
    city = Column(VARCHAR(100))
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


class OrganizationCategory(Base):
    __tablename__ = 'crunchbase_organizations_categories'

    organization_id = Column(VARCHAR(50), ForeignKey('crunchbase_organizations.id'), primary_key=True)
    category_name = Column(VARCHAR(100), ForeignKey('crunchbase_category_groups.category_name'), primary_key=True)


class CategoryGroup(Base):
    __tablename__ = 'crunchbase_category_groups'

    id = Column(VARCHAR(50))
    category_name = Column(VARCHAR(100), primary_key=True)
    category_group_list = Column(VARCHAR(150))


class Acquisition(Base):
    __tablename__ = 'crunchbase_acquisitions'

    acquisition_id = Column(VARCHAR(36), primary_key=True)
    acquiree_name = Column(VARCHAR(200))
    acquiree_country_code = Column(VARCHAR(3))
    state_code = Column(VARCHAR(2))
    acquiree_region = Column(VARCHAR(50))
    acquiree_city = Column(VARCHAR(50))
    acquirer_name = Column(VARCHAR(200))
    acquirer_country_code = Column(VARCHAR(3))
    acquirer_state_code = Column(VARCHAR(2))
    acquirer_region = Column(VARCHAR(50))
    acquirer_city = Column(VARCHAR(50))
    acquisition_type = Column(VARCHAR(20))
    acquired_on = Column(DATE)
    price_usd = Column(BIGINT)
    price = Column(BIGINT)
    price_currency_code = Column(VARCHAR(3))
    acquiree_cb_url = Column(TEXT)
    acquirer_cb_url = Column(TEXT)
    acquiree_id = Column(VARCHAR(36))
    acquirer_id = Column(VARCHAR(36))
    created_at = Column(DATETIME)
    updated_at = Column(DATETIME)


class Degree(Base):
    __tablename__ = 'crunchbase_degrees'

    degree_id = Column(VARCHAR(36), primary_key=True)
    institution_id = Column(VARCHAR(36))
    person_id = Column(VARCHAR(36))
    degree_type = Column(VARCHAR(100))
    subject = Column(VARCHAR(100))
    started_on = Column(VARCHAR(10))
    completed_on = Column(VARCHAR(10))
    is_completed = Column(BOOLEAN)
    created_at = Column(DATETIME)
    updated_at = Column(DATETIME)


class FundingRound(Base):
    __tablename__ = 'crunchbase_funding_rounds'

    funding_round_id = Column(VARCHAR(36), primary_key=True)
    company_name = Column(VARCHAR(200))
    location_id = Column(VARCHAR(400), index=True)
    country = Column(VARCHAR(200))
    state_code = Column(VARCHAR(2))
    region = Column(VARCHAR(100))
    city = Column(VARCHAR(100))
    investment_type = Column(VARCHAR(30))
    announced_on = Column(DATE)
    raised_amount_usd = Column(BIGINT)
    raised_amount = Column(BIGINT)
    raised_amount_currency_code = Column(VARCHAR(3))
    post_money_valuation_usd = Column(BIGINT)
    post_money_valuation = Column(BIGINT)
    post_money_currency_code = Column(VARCHAR(3))
    investor_count = Column(BIGINT)
    cb_url = Column(TEXT)
    company_id = Column(VARCHAR(36))
    created_at = Column(DATETIME)
    updated_at = Column(DATETIME)
    investor_names = Column(TEXT)
    investor_ids = Column(TEXT)


class Fund(Base):
    __tablename__ = 'crunchbase_funds'

    fund_id = Column(VARCHAR(36), primary_key=True)
    entity_id = Column(VARCHAR(36))
    fund_name = Column(VARCHAR(200))
    announced_on = Column(DATE)
    raised_amount = Column(BIGINT)
    raised_amount_currency_code = Column(VARCHAR(3))
    created_at = Column(DATETIME)
    updated_at = Column(DATETIME)


class InvestmentPartner(Base):
    __tablename__ = 'crunchbase_investment_partners'

    funding_round_id = Column(VARCHAR(36), primary_key=True)
    investor_id = Column(VARCHAR(36), primary_key=True)
    partner_id = Column(VARCHAR(36), primary_key=True)


class Investment(Base):
    __tablename__ = 'crunchbase_investments'

    funding_round_id = Column(VARCHAR(36), primary_key=True)
    investor_id = Column(VARCHAR(36), primary_key=True)
    is_lead_investor = Column(BOOLEAN)


class Investor(Base):
    __tablename__ = 'crunchbase_investors'

    id = Column(VARCHAR(36), primary_key=True)
    investor_name = Column(VARCHAR(200))
    roles = Column(VARCHAR(23))
    domain = Column(VARCHAR(80))
    location_id = Column(VARCHAR(400), index=True)
    country = Column(VARCHAR(200))
    state_code = Column(VARCHAR(2))
    region = Column(VARCHAR(2))
    city = Column(VARCHAR(100))
    investor_type = Column(VARCHAR(25))
    investment_count = Column(BIGINT)
    total_funding_usd = Column(BIGINT)
    founded_on = Column(DATE)
    closed_on = Column(DATE)
    cb_url = Column(TEXT)
    logo_url = Column(TEXT)
    twitter_url = Column(TEXT)
    facebook_url = Column(TEXT)
    updated_at = Column(DATETIME)


class Ipo(Base):
    __tablename__ = 'crunchbase_ipos'

    ipo_id = Column(VARCHAR(36), primary_key=True)
    name = Column(VARCHAR(100))
    company_state_code = Column(VARCHAR(2))
    location_id = Column(VARCHAR(400), index=True)
    country = Column(VARCHAR(200))
    region = Column(VARCHAR(100))
    city = Column(VARCHAR(100))
    stock_exchange_symbol = Column(VARCHAR(10))
    stock_symbol = Column(VARCHAR(12))
    went_public_on = Column(DATE)
    price_usd = Column(BIGINT)
    price = Column(BIGINT)
    price_currency_code = Column(VARCHAR(3))
    money_raised_usd = Column(BIGINT)
    cb_url = Column(TEXT)
    company_id = Column(VARCHAR(36))
    created_at = Column(DATETIME)
    updated_at = Column(DATETIME)


class Job(Base):
    __tablename__ = 'crunchbase_jobs'

    job_id = Column(VARCHAR(36), primary_key=True)
    person_id = Column(VARCHAR(36))
    org_id = Column(VARCHAR(36))
    started_on = Column(DATE)
    ended_on = Column(DATE)
    is_current = Column(BOOLEAN)
    title = Column(VARCHAR(150))
    job_type = Column(VARCHAR(20))


class People(Base):
    __tablename__ = 'crunchbase_people'

    id = Column(VARCHAR(36), primary_key=True)
    first_name = Column(VARCHAR(100))
    last_name = Column(VARCHAR(100))
    location_id = Column(VARCHAR(400), index=True)
    country = Column(VARCHAR(200))
    state_code = Column(VARCHAR(2))
    city = Column(VARCHAR(100))
    cb_url = Column(TEXT)
    logo_url = Column(TEXT)
    twitter_url = Column(TEXT)
    facebook_url = Column(TEXT)
    linkedin_url = Column(TEXT)
    primary_affiliation_organization = Column(VARCHAR(100))
    primary_affiliation_title = Column(VARCHAR(100))
    primary_organization_id = Column(VARCHAR(36))
    gender = Column(VARCHAR(20))
    created_at = Column(DATETIME)
    updated_at = Column(DATETIME)
