'''
General ORM
===========

ORMs for curated data, to be transfered to the general ES endpoint.
'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import VARCHAR, BIGINT, TEXT, JSON
from sqlalchemy.types import INT, DATE, DATETIME, BOOLEAN
from sqlalchemy import Column
from nesta.core.orms.crunchbase_orm import fixture as cb_fixture
from nesta.core.orms.crunchbase_orm import _TEXT

Base = declarative_base()

class CrunchbaseOrg(Base):
    __tablename__ = 'curated_crunchbase_organizations'

    # Fields ported from crunchbase_orm.Organization
    id = cb_fixture('id_pk')
    address = Column(VARCHAR(200, collation='utf8mb4_unicode_ci'))
    cb_url = cb_fixture('cb_url')
    city = cb_fixture('city')
    closed_on = cb_fixture('happened_on')
    country = cb_fixture('country')
    employee_count = cb_fixture('country')
    facebook_url = cb_fixture('url')
    founded_on = cb_fixture('happened_on')
    homepage_url = cb_fixture('url')
    last_funding_on = cb_fixture('happened_on')
    linkedin_url = cb_fixture('url')
    long_description = Column(_TEXT)
    name = cb_fixture('name')
    num_exits = Column(INT)
    num_funding_rounds = Column(INT)
    parent_id = cb_fixture('id_idx')
    primary_role = Column(VARCHAR(50))
    region = cb_fixture('region')
    roles = cb_fixture('roles')
    short_description = Column(VARCHAR(200, collation='utf8mb4_unicode_ci'))
    state_code = cb_fixture('state_code')
    status = Column(VARCHAR(9))
    total_funding_usd = cb_fixture('monetary_amount')
    twitter_url = cb_fixture('url')

    # New, joined or updated fields
    aliases = Column(JSON)
    category_list = Column(JSON)
    category_groups_list = Column(JSON)
    coordinates = Column(JSON)
    continent_alpha_2 = cb_fixture('state_code')
    continent_name = Column(VARCHAR(20))
    country_alpha_2 = cb_fixture('state_code')
    country_alpha_3 = cb_fixture('iso3')
    country_numeric = cb_fixture('iso3')
    country_mentions = Column(JSON)
    investor_names = Column(JSON)
    is_eu = Column(BOOLEAN, nullable=False)
    state_name = cb_fixture('name')
    updated_at = cb_fixture('happened_on')
