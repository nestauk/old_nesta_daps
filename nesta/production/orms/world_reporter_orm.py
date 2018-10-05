'''
World RePORTER
==============

The schema for the World RePORTER data. Note that the schema
was automatically generated based on an IPython hack session
using the limits and properties of the data from the 
World ExPORTER (which explains the unusual VARCHAR limits).
'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import VARCHAR, TEXT
from sqlalchemy.types import INTEGER
from sqlalchemy import Column


Base = declarative_base()

class Projects(Base):
    __tablename__ = 'nih_projects'

    application_id = Column(INTEGER, primary_key=True)
    activity = Column(VARCHAR(3))
    administering_ic = Column(VARCHAR(2))
    application_type = Column(INTEGER)
    arra_funded = Column(VARCHAR(1))
    award_notice_date = Column(VARCHAR(19))
    budget_start = Column(VARCHAR(10))
    budget_end = Column(VARCHAR(10))
    cfda_code = Column(VARCHAR(23))
    core_project_num = Column(VARCHAR(32), index=True)
    ed_inst_type = Column(VARCHAR(70))
    foa_number = Column(VARCHAR(14))
    full_project_num = Column(VARCHAR(35))
    funding_ics = Column(VARCHAR(291))
    funding_mechanism = Column(VARCHAR(23))
    fy = Column(INTEGER)
    ic_name = Column(VARCHAR(79))
    org_city = Column(VARCHAR(26))
    org_country = Column(VARCHAR(14))
    org_dept = Column(VARCHAR(30))
    org_district = Column(INTEGER)
    org_duns = Column(VARCHAR(75))
    org_fips = Column(VARCHAR(2))
    org_ipf_code = Column(INTEGER)
    org_name = Column(VARCHAR(92))
    org_state = Column(VARCHAR(2))
    org_zipcode = Column(VARCHAR(10))
    phr = Column(TEXT)
    pi_ids = Column(VARCHAR(262))
    pi_names = Column(VARCHAR(533))
    program_officer_name = Column(VARCHAR(39))
    project_start = Column(VARCHAR(10))
    project_end = Column(VARCHAR(10))
    project_terms = Column(TEXT)
    project_title = Column(VARCHAR(200))
    serial_number = Column(VARCHAR(6))
    study_section = Column(VARCHAR(4))
    study_section_name = Column(VARCHAR(95))
    suffix = Column(VARCHAR(6))
    support_year = Column(VARCHAR(2))
    direct_cost_amt = Column(INTEGER)
    indirect_cost_amt = Column(INTEGER)
    total_cost = Column(INTEGER)
    subproject_id = Column(INTEGER)
    total_cost_sub_project = Column(INTEGER)
    nih_spending_cats = Column(VARCHAR(2232))


class Abstracts(Base):
    __tablename__ = 'nih_abstracts'

    application_id = Column(INTEGER, primary_key=True)
    abstract_text = Column(TEXT)

class Publications(Base):
    __tablename__ = 'nih_publications'

    pmid = Column(INTEGER, primary_key=True)
    author_name = Column(VARCHAR(166))
    affiliation = Column(TEXT)
    author_list = Column(TEXT)
    country = Column(VARCHAR(25))
    issn = Column(VARCHAR(9))
    journal_issue = Column(VARCHAR(75))
    journal_title = Column(VARCHAR(282))
    journal_title_abbr = Column(VARCHAR(108))
    journal_volume = Column(VARCHAR(100))
    lang = Column(VARCHAR(3))
    page_number = Column(VARCHAR(138))
    pub_date = Column(VARCHAR(23))
    pub_title = Column(TEXT)
    pub_year = Column(INTEGER)
    pmc_id = Column(INTEGER)
     
class Patents(Base):
    __tablename__ = 'nih_patents'
    
    patent_id = Column(VARCHAR(9), primary_key=True)
    patent_title = Column(VARCHAR(284))
    project_id = Column(VARCHAR(11), index=True)
    patent_org_name = Column(VARCHAR(50))

class LinkTables(Base):
    __tablename__ = 'nih_linktables'

    pmid = Column(INTEGER, primary_key=True)
    project_number = Column(VARCHAR(11), index=True)
