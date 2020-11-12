'''
General ORM
===========

ORMs for curated data, to be transfered to the general ES endpoint.
'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import BIGINT, JSON
from sqlalchemy.types import INTEGER, DATE, DATETIME, BOOLEAN
from sqlalchemy import Column
from nesta.core.orms.crunchbase_orm import fixture as cb_fixture
from nesta.core.orms.types import TEXT, VARCHAR


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
    long_description = Column(TEXT)
    name = cb_fixture('name')
    num_exits = Column(INTEGER)
    num_funding_rounds = Column(INTEGER)
    parent_id = cb_fixture('id_idx')
    primary_role = Column(VARCHAR(50))
    region = cb_fixture('region')
    roles = cb_fixture('roles')
    short_description = Column(VARCHAR(200))
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


class NihProject(Base):
    __tablename__ = 'curated_nih_projects'

    # Fields ported from nih_orm.Projects
    application_id = Column(INTEGER, primary_key=True, autoincrement=False)
    full_project_num = Column(VARCHAR(50), index=True)
    fy = Column(INTEGER, index=True)
    org_city = Column(VARCHAR(50), index=True)
    org_country = Column(VARCHAR(50), index=True)
    org_name = Column(VARCHAR(100), index=True)
    org_state = Column(VARCHAR(2), index=True)
    org_zipcode = Column(VARCHAR(10))
    project_terms = Column(JSON)
    project_title = Column(TEXT)
    phr = Column(TEXT)
    ic_name = Column(VARCHAR(100), index=True)

    # Fields from other ORMs in nih_orm
    abstract_text = Column(TEXT)

    # New, joined or updated fields
    clinicaltrials = Column(JSON)
    continent_iso2 = Column(VARCHAR(2), index=True)
    continent = Column(TEXT)
    coordinate = Column(JSON)
    countryTags = Column(JSON)
    currency = Column(VARCHAR(3), default="USD", index=True)
    fairly_similar_ids = Column(JSON)
    is_eu = Column(BOOLEAN, nullable=False)
    iso2 = Column(VARCHAR(2), index=True)
    iso3 = Column(VARCHAR(3), index=True)
    isoNumeric = Column(INTEGER, index=True)
    near_duplicate_ids = Column(JSON)
    patent_ids = Column(JSON)
    patent_titles = Column(JSON)    
    pmids = Column(JSON)
    project_start = Column(DATETIME, index=True)
    project_end = Column(DATETIME, index=True)
    grouped_ids = Column(JSON)
    grouped_titles = Column(JSON)
    total_cost = Column(BIGINT)
    very_similar_ids = Column(JSON)
    yearly_funds = Column(JSON)
    
