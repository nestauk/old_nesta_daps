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
_TEXT = TEXT(collation='utf8mb4_unicode_ci')
FIXTURES = {'cb_url': lambda: Column(VARCHAR(200)),
            'city': lambda: Column(VARCHAR(100, collation='utf8mb4_unicode_ci')),
            'country': lambda: Column(VARCHAR(200, collation='utf8mb4_unicode_ci')),
            'currency_code': lambda: Column(VARCHAR(3)),             
            'url': lambda: Column(_TEXT),
            'happened_on': lambda: Column(DATE),
            'id_pk': lambda: Column(VARCHAR(50), primary_key=True),
            'id_idx': lambda: Column(VARCHAR(50), index=True),
            'iso3': lambda: Column(VARCHAR(3)),
            'job_title': lambda: Column(VARCHAR(150)),             
            'location_id': lambda: Column(VARCHAR(400, collation='utf8mb4_unicode_ci'), index=True),
            'monetary_amount': lambda: Column(BIGINT),
            'name': lambda: Column(VARCHAR(250, collation='utf8mb4_unicode_ci')),
            'rank': lambda: Column(BIGINT),
            'region': lambda: Column(VARCHAR(100, collation='utf8mb4_unicode_ci')),
            'roles': lambda: Column(VARCHAR(50)),
            'state_code': lambda: Column(VARCHAR(2)),
            'type': lambda: Column(VARCHAR(50)),
            'timestamp': lambda: Column(DATETIME)}  # {create/updated}_at}


def fixture(key):
    return FIXTURES[key]()


class Organization(Base):
    __tablename__ = 'crunchbase_organizations'

    id = fixture('id_pk')
    address = Column(VARCHAR(200, collation='utf8mb4_unicode_ci'))
    alias1 = Column(_TEXT)
    alias2 = Column(_TEXT)
    alias3 = Column(_TEXT)
    categories = relationship('CategoryGroup',
                              secondary='crunchbase_organizations_categories')
    cb_url = fixture('cb_url')
    city = fixture('city')
    closed_on = fixture('happened_on')
    country = fixture('country')
    country_code = fixture('iso3')
    created_at = fixture('timestamp')
    domain = Column(_TEXT)
    email = Column(_TEXT)
    employee_count = Column(_TEXT)
    facebook_url = fixture('url')
    founded_on = fixture('happened_on')
    homepage_url = fixture('url')
    is_health = Column(BOOLEAN)
    last_funding_on = fixture('happened_on')
    legal_name = fixture('name')
    linkedin_url = fixture('url')
    location_id = fixture('location_id')
    logo_url = fixture('url')
    long_description = Column(_TEXT)
    mesh_terms = Column(_TEXT)
    name = fixture('name')
    num_exits = Column(INT)
    num_funding_rounds = Column(INT)
    parent_id = fixture('id_idx')
    permalink = fixture('url')
    phone = Column(_TEXT)
    postal_code = Column(VARCHAR(30, collation='utf8mb4_unicode_ci'))
    primary_role = Column(VARCHAR(50))
    rank = fixture('rank')
    region = fixture('region')
    roles = fixture('roles')
    short_description = Column(VARCHAR(200, collation='utf8mb4_unicode_ci'))
    state_code = fixture('state_code')
    status = Column(VARCHAR(9))
    total_funding = fixture('monetary_amount')
    total_funding_currency_code = fixture('currency_code')
    total_funding_usd = fixture('monetary_amount')
    twitter_url = fixture('url')
    type = fixture('type')
    updated_at = fixture('timestamp')


class OrganizationCategory(Base):
    __tablename__ = 'crunchbase_organizations_categories'

    category_name = Column(VARCHAR(100), ForeignKey('crunchbase_category_groups.name'), primary_key=True)
    organization_id = Column(VARCHAR(50), ForeignKey('crunchbase_organizations.id'), primary_key=True)


class CategoryGroup(Base):
    __tablename__ = 'crunchbase_category_groups'

    id = fixture('id_idx')
    name = Column(VARCHAR(100), primary_key=True)
    category_groups_list = Column(VARCHAR(150))


class Acquisition(Base):
    __tablename__ = 'crunchbase_acquisitions'

    id = fixture('id_pk')
    acquired_on = fixture('happened_on')
    acquiree_cb_url = fixture('cb_url')
    acquiree_city = fixture('city')
    acquiree_country_code = fixture('iso3')
    acquiree_id = fixture('id_idx')
    acquiree_name = fixture('name')
    acquiree_region = fixture('region')
    acquiree_state_code = fixture('state_code')
    acquirer_cb_url = fixture('cb_url')
    acquirer_city = fixture('city')
    acquirer_country_code = fixture('iso3')
    acquirer_id = fixture('id_idx')
    acquirer_name = fixture('name')
    acquirer_region = fixture('region')
    acquirer_state_code = fixture('state_code')
    acquisition_type = Column(VARCHAR(20))
    cb_url = fixture('cb_url')
    created_at = fixture('timestamp')
    name = fixture('name')
    permalink = fixture('url')
    price = fixture('monetary_amount')
    price_currency_code = fixture('currency_code')
    price_usd = fixture('monetary_amount')
    rank = fixture('rank')
    type = fixture('type')
    updated_at = fixture('timestamp')


class Degree(Base):
    __tablename__ = 'crunchbase_degrees'

    id = fixture('id_pk')
    cb_url = fixture('cb_url')
    completed_on = fixture('happened_on')
    created_at = fixture('timestamp')
    degree_type = Column(VARCHAR(100))
    institution_id = fixture('id_idx')
    institution_name = fixture('name')
    is_completed = Column(BOOLEAN)
    name = fixture('name')
    permalink = fixture('url')
    person_id = fixture('id_idx')
    person_name = fixture('name')
    rank = fixture('rank')
    started_on = fixture('happened_on')
    subject = Column(VARCHAR(100))
    type = fixture('type')
    updated_at = fixture('timestamp')


class FundingRound(Base):
    __tablename__ = 'crunchbase_funding_rounds'

    id = fixture('id_pk')
    announced_on = fixture('happened_on')
    cb_url = fixture('cb_url')
    city = fixture('city')
    country = fixture('country')
    country_code = fixture('iso3')
    created_at = fixture('timestamp')
    investment_type = Column(VARCHAR(30))
    investor_count = Column(BIGINT)
    lead_investor_ids = Column(_TEXT)
    location_id = fixture('location_id')
    name = fixture('name')
    org_id = fixture('id_idx')
    org_name = fixture('name')
    permalink = fixture('url')
    post_money_valuation = fixture('monetary_amount')
    post_money_valuation_currency_code = fixture('currency_code')
    post_money_valuation_usd = fixture('monetary_amount')
    raised_amount = fixture('monetary_amount')
    raised_amount_currency_code = fixture('currency_code')
    raised_amount_usd = fixture('monetary_amount')
    rank = fixture('rank')
    region = fixture('region')
    state_code = fixture('state_code')
    type = fixture('type')
    updated_at = fixture('timestamp')


class Fund(Base):
    __tablename__ = 'crunchbase_funds'

    id = fixture('id_pk')
    announced_on = fixture('happened_on')
    cb_url = fixture('cb_url')
    created_at = fixture('timestamp')
    entity_id = fixture('id_idx')
    entity_name = fixture('name')
    entity_type = fixture('type')
    name = fixture('name')
    permalink = fixture('url')
    raised_amount = fixture('monetary_amount')
    raised_amount_currency_code = fixture('currency_code')
    raised_amount_usd = fixture('monetary_amount')
    rank = fixture('rank')
    type = fixture('type')
    updated_at = fixture('timestamp')


class InvestmentPartner(Base):
    __tablename__ = 'crunchbase_investment_partners'

    id = fixture('id_pk')
    cb_url = fixture('cb_url')
    created_at = fixture('timestamp')
    funding_round_id = fixture('id_idx')
    funding_round_name = fixture('name')
    investor_id = fixture('id_idx')
    investor_name = fixture('name')
    name = fixture('name')
    partner_id = fixture('id_idx')
    partner_name = fixture('name')
    permalink = fixture('url')
    rank = fixture('rank')
    type = fixture('type')
    updated_at = fixture('timestamp')


class Investment(Base):
    __tablename__ = 'crunchbase_investments'

    id = fixture('id_pk')
    cb_url = fixture('cb_url')
    created_at = fixture('timestamp')
    funding_round_id = fixture('id_idx')
    funding_round_name = fixture('name')
    investor_id = fixture('id_idx')
    investor_name = fixture('name')
    investor_type = fixture('type')
    is_lead_investor = Column(BOOLEAN)
    name = fixture('name')
    partner_name = fixture('name')
    permalink = fixture('url')
    rank = fixture('rank')
    type = fixture('type')
    updated_at = fixture('timestamp')


class Investor(Base):
    __tablename__ = 'crunchbase_investors'

    id = fixture('id_pk')
    cb_url = fixture('cb_url')
    city = fixture('city')
    closed_on = fixture('happened_on')
    country = fixture('country')
    country_code = fixture('iso3')
    created_at = fixture('timestamp')
    domain = Column(VARCHAR(80))
    facebook_url = fixture('url')
    founded_on = fixture('happened_on')
    investment_count = Column(BIGINT)
    investor_types = Column(_TEXT)
    linkedin_url = fixture('url')
    location_id = fixture('location_id')
    logo_url = fixture('url')
    name = fixture('name')
    permalink = fixture('url')
    rank = fixture('rank')
    region = fixture('region')
    roles = fixture('roles')
    state_code = fixture('state_code')
    total_funding = fixture('monetary_amount')
    total_funding_currency_code = fixture('currency_code')
    total_funding_usd = fixture('monetary_amount')
    twitter_url = fixture('url')
    type = fixture('type')
    updated_at = fixture('timestamp')


class Ipo(Base):
    __tablename__ = 'crunchbase_ipos'

    id = fixture('id_pk')
    cb_url = fixture('cb_url')
    city = fixture('city')
    country = fixture('country')
    country_code = fixture('iso3')
    created_at = fixture('timestamp')
    location_id = fixture('location_id')
    money_raised = fixture('monetary_amount')
    money_raised_currency_code = fixture('currency_code')
    money_raised_usd = fixture('monetary_amount')
    name = fixture('name')
    org_cb_url = fixture('cb_url')
    org_id = fixture('id_idx')
    org_name = fixture('name')
    permalink = fixture('url')
    rank = fixture('rank')
    region = fixture('region')
    share_price = fixture('monetary_amount')
    share_price_currency_code = fixture('currency_code')
    share_price_usd = fixture('monetary_amount')
    state_code = fixture('state_code')
    stock_exchange_symbol = Column(VARCHAR(10))
    stock_symbol = Column(VARCHAR(12))
    type = fixture('type')
    updated_at = fixture('timestamp')
    valuation_price = fixture('monetary_amount')
    valuation_price_currency_code = fixture('currency_code')
    valuation_price_usd = fixture('monetary_amount')
    went_public_on = fixture('happened_on')


class Job(Base):
    __tablename__ = 'crunchbase_jobs'

    id = fixture('id_pk')
    cb_url = fixture('cb_url')
    created_at = fixture('timestamp')
    ended_on = fixture('happened_on')
    is_current = Column(BOOLEAN)
    job_type = Column(VARCHAR(20))
    name = fixture('name')
    org_id = fixture('id_idx')
    org_name = fixture('name')
    permalink = fixture('url')
    person_id = fixture('id_idx')
    person_name = fixture('name')
    rank = fixture('rank')
    started_on = fixture('happened_on')
    title = fixture('job_title')
    type = fixture('type')
    updated_at = fixture('timestamp')


class People(Base):
    __tablename__ = 'crunchbase_people'

    id = fixture('id_pk')
    cb_url = fixture('cb_url')
    city = fixture('city')
    country = fixture('country')
    country_code = fixture('iso3')
    created_at = fixture('timestamp')
    facebook_url = fixture('url')
    featured_job_organization_id = fixture('id_idx')
    featured_job_organization_name = fixture('name')
    featured_job_title = fixture('job_title')
    first_name = fixture('name')
    gender = Column(VARCHAR(20))
    last_name = fixture('name')
    linkedin_url = fixture('url')
    location_id = fixture('location_id')
    logo_url = fixture('url')
    name = fixture('name')
    permalink = fixture('url')
    rank = fixture('rank')
    region = fixture('region')
    state_code = fixture('state_code')
    twitter_url = fixture('url')
    type = fixture('type')
    updated_at = fixture('timestamp')
