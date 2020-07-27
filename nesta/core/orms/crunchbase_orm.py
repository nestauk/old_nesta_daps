'''
Crunchbase
================
'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import VARCHAR, BIGINT, TEXT
from sqlalchemy.types import INT, DATE, DATETIME, BOOLEAN
from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import relationship

Base = declarative_base()

# There are a lot of repeated data types in this schema, so the following
# fixtures are designed to maintain consistency between tables
FIXTURES = {'permalink': lambda: Column(VARCHAR(100)),
            'cb_url': lambda: Column(VARCHAR(200)),
            'rank': lambda: Column(BIGINT),
            'type': lambda: Column(VARCHAR(50)),
            'created_at': lambda: Column(DATETIME),  # also "updated_at"
            'external_url': lambda: Column(TEXT),
            'name': lambda: Column(VARCHAR(200, collation='utf8mb4_unicode_ci')),
            'iso3': lambda: Column(VARCHAR(3)),
            'country': lambda: Column(VARCHAR(200, collation='utf8mb4_unicode_ci')),
            'state_code': lambda: Column(VARCHAR(2)),
            'region': lambda: Column(VARCHAR(100, collation='utf8mb4_unicode_ci')),
            'city': lambda: Column(VARCHAR(100, collation='utf8mb4_unicode_ci')),
            'location_id': lambda: Column(VARCHAR(400, collation='utf8mb4_unicode_ci'), index=True),
            'happened_on': lambda: Column(DATE),
            'id_pk': lambda: Column(VARCHAR(36), primary_key=True),
            'id_idx': lambda: Column(VARCHAR(36), index=True),
            'currency_code': lambda: Column(VARCHAR(3))}


def fixture(key):
    return FIXTURES[key]()


class Organization(Base):
    __tablename__ = 'crunchbase_organizations'

    id = fixture('id_pk')
    name = fixture('name')
    roles = Column(VARCHAR(50))
    permalink = fixture('permalink')
    domain = Column(TEXT)
    homepage_url = fixture('external_url')
    location_id = fixture('location_id')
    country = fixture('country')
    country_code = fixture('iso3')
    state_code = fixture('state_code')
    region = fixture('region')
    city = fixture('city')
    address = Column(VARCHAR(200, collation='utf8mb4_unicode_ci'))
    status = Column(VARCHAR(9))
    short_description = Column(VARCHAR(200, collation='utf8mb4_unicode_ci'))
    num_funding_rounds = Column(INT)
    total_funding_usd = Column(BIGINT)
    founded_on = fixture('happened_on')
    last_funding_on = fixture('happened_on')
    closed_on = fixture('happened_on')
    employee_count = Column(BIGINT)
    email = Column(VARCHAR(200, collation='utf8mb4_unicode_ci'))
    phone = Column(TEXT)
    facebook_url = fixture('external_url')
    linkedin_url = fixture('external_url')
    cb_url = fixture('cb_url')
    logo_url = fixture('external_url')
    twitter_url = fixture('external_url')
    alias1 = fixture('name')
    alias2 = fixture('name')
    alias3 = fixture('name')
    created_at = fixture('created_at')
    updated_at = fixture('created_at')
    primary_role = Column(VARCHAR(50))
    type = fixture('type')
    legal_name = fixture('name')
    total_funding = Column(BIGINT)
    total_funding_currency_code = fixture('currency_code')
    num_exits = Column(INT)
    postal_code = Column(VARCHAR(30, collation='utf8mb4_unicode_ci'))
    rank = fixture('rank')
    long_description = Column(TEXT(collation='utf8mb4_unicode_ci'))
    parent_id = Column(VARCHAR(50))
    is_health = Column(BOOLEAN)
    mesh_terms = Column(TEXT)
    categories = relationship('CategoryGroup',
                              secondary='crunchbase_organizations_categories')


class OrganizationCategory(Base):
    __tablename__ = 'crunchbase_organizations_categories'

    organization_id = Column(VARCHAR(36), ForeignKey('crunchbase_organizations.id'), primary_key=True)
    category_name = Column(VARCHAR(100), ForeignKey('crunchbase_category_groups.name'), primary_key=True)


class CategoryGroup(Base):
    __tablename__ = 'crunchbase_category_groups'

    id = fixture('id_idx')
    name = Column(VARCHAR(100), primary_key=True)
    category_groups_list = Column(VARCHAR(150))


class Acquisition(Base):
    __tablename__ = 'crunchbase_acquisitions'

    acquisition_id = fixture('id_pk')
    acquiree_name = fixture('name')
    acquiree_country_code = fixture('iso3')
    state_code = fixture('state_code')
    acquiree_region = fixture('region')
    acquiree_city = fixture('city')
    acquirer_name = fixture('name')
    acquirer_country_code = fixture('iso3')
    acquirer_state_code = fixture('state_code')
    acquirer_region = fixture('region')
    acquirer_city = fixture('city')
    acquisition_type = Column(VARCHAR(20))
    acquired_on = fixture('happened_on')
    price_usd = Column(BIGINT)
    price = Column(BIGINT)
    price_currency_code = fixture('currency_code')
    acquiree_cb_url = fixture('cb_url')
    acquirer_cb_url = fixture('cb_url')
    acquiree_id = fixture('id_idx')
    acquirer_id = fixture('id_idx')
    created_at = fixture('created_at')
    updated_at = fixture('created_at')


class Degree(Base):
    __tablename__ = 'crunchbase_degrees'

    degree_id = fixture('id_pk')
    institution_id = fixture('id_idx')
    person_id = fixture('id_idx')
    degree_type = Column(VARCHAR(100))
    subject = Column(VARCHAR(100))
    started_on = fixture('happened_on')
    completed_on = fixture('happened_on')
    is_completed = Column(BOOLEAN)
    created_at = fixture('created_at')
    updated_at = fixture('created_at')


class FundingRound(Base):
    __tablename__ = 'crunchbase_funding_rounds'

    funding_round_id = fixture('id_pk')
    company_name = fixture('name')
    location_id = fixture('location_id')
    country = fixture('country')
    country_code = fixture('iso3')
    state_code = fixture('state_code')
    region = fixture('region')
    city = fixture('city')
    investment_type = Column(VARCHAR(30))
    announced_on = fixture('happened_on')
    raised_amount_usd = Column(BIGINT)
    raised_amount = Column(BIGINT)
    raised_amount_currency_code = fixture('currency_code')
    post_money_valuation_usd = Column(BIGINT)
    post_money_valuation = Column(BIGINT)
    post_money_currency_code = fixture('currency_code')
    investor_count = Column(BIGINT)
    cb_url = fixture('cb_url')
    company_id = fixture('id_idx')
    created_at = fixture('created_at')
    updated_at = fixture('created_at')
    investor_names = Column(TEXT(collation='utf8mb4_unicode_ci'))
    investor_ids = Column(TEXT)


class Fund(Base):
    __tablename__ = 'crunchbase_funds'

    fund_id = fixture('id_pk')
    entity_id = fixture('id_idx')
    fund_name = fixture('name')
    announced_on = fixture('happened_on')
    raised_amount = Column(BIGINT)
    raised_amount_currency_code = fixture('currency_code')
    created_at = fixture('created_at')
    updated_at = fixture('created_at')


class InvestmentPartner(Base):
    __tablename__ = 'crunchbase_investment_partners'

    funding_round_id = fixture('id_pk')
    investor_id = fixture('id_pk')
    partner_id = fixture('id_pk')


class Investment(Base):
    __tablename__ = 'crunchbase_investments'

    funding_round_id = fixture('id_pk')
    investor_id = fixture('id_pk')
    is_lead_investor = Column(BOOLEAN)


class Investor(Base):
    __tablename__ = 'crunchbase_investors'

    id = fixture('id_pk')
    investor_name = fixture('name')
    roles = Column(VARCHAR(23))
    domain = Column(VARCHAR(80))
    location_id = fixture('location_id')
    country = fixture('country')
    country_code = fixture('iso3')
    state_code = fixture('state_code')
    region = fixture('region')
    city = fixture('city')
    investor_type = Column(VARCHAR(25))
    investment_count = Column(BIGINT)
    total_funding_usd = Column(BIGINT)
    founded_on = fixture('happened_on')
    closed_on = fixture('happened_on')
    cb_url = fixture('cb_url')
    logo_url = fixture('external_url')
    twitter_url = fixture('external_url')
    facebook_url = fixture('external_url')
    updated_at = Column(DATETIME)


class Ipo(Base):
    __tablename__ = 'crunchbase_ipos'

    ipo_id = fixture('id_pk')
    name = fixture('name')
    company_state_code = fixture('state_code')
    location_id = fixture('location_id')
    country = fixture('country')
    country_code = fixture('iso3')
    region = fixture('region')
    city = fixture('city')
    stock_exchange_symbol = Column(VARCHAR(10))
    stock_symbol = Column(VARCHAR(12))
    went_public_on = fixture('happened_on')
    price_usd = Column(BIGINT)
    price = Column(BIGINT)
    price_currency_code = fixture('currency_code')
    money_raised_usd = Column(BIGINT)
    cb_url = fixture('cb_url')
    company_id = fixture('id_idx')
    created_at = fixture('created_at')
    updated_at = fixture('created_at')


class Job(Base):
    __tablename__ = 'crunchbase_jobs'

    job_id = fixture('id_pk')
    person_id = fixture('id_idx')
    org_id = fixture('id_idx')
    started_on = fixture('happened_on')
    ended_on = fixture('happened_on')
    is_current = Column(BOOLEAN)
    title = Column(VARCHAR(150))
    job_type = Column(VARCHAR(20))


class People(Base):
    __tablename__ = 'crunchbase_people'

    id = fixture('id_pk')
    first_name = fixture('name')
    last_name = fixture('name')
    location_id = fixture('location_id')
    country = fixture('country')
    country_code = fixture('iso3')
    state_code = fixture('state_code')
    city = fixture('city')
    cb_url = fixture('cb_url')
    logo_url = fixture('external_url')
    twitter_url = fixture('external_url')
    facebook_url = fixture('external_url')
    linkedin_url = fixture('external_url')
    primary_affiliation_organization = fixture('name')
    primary_affiliation_title = fixture('name')
    primary_organization_id = fixture('id_idx')
    gender = Column(VARCHAR(20))
    created_at = fixture('created_at')
    updated_at = fixture('created_at')
