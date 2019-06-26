'''
CORDIS H2020
============
'''

from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import VARCHAR, TEXT
from sqlalchemy.types import DATETIME, INTEGER, FLOAT, BOOLEAN

Base = declarative_base()


class ProjectProgrammes(Base):
    __tablename__ = 'cordisH2020_project_programmes'
    project_rcn = Column(INTEGER, 
                         ForeignKey('cordisH2020_projects.rcn'), 
                         primary_key=True)
    programme_code = Column(VARCHAR(19, collation='utf8_bin'),
                            ForeignKey('cordisH2020_programmes.id'), 
                            primary_key=True)

class Programme(Base):
    __tablename__ = 'cordisH2020_programmes'
    id = Column(VARCHAR(19, collation='utf8_bin'), primary_key=True)
    framework_programme = Column(VARCHAR(5, collation='utf8_bin'))

class Organizations(Base):
    __tablename__ = 'cordisH2020_organizations'
    project_rcn = Column(INTEGER, index=True)
    #project_id = Column(INTEGER)
    #project_acronym = Column(TEXT(collation='utf8_bin'))
    role = Column(VARCHAR(15, collation='utf8_bin'))
    id = Column(INTEGER, primary_key=True, autoincrement=False)
    name = Column(TEXT(collation='utf8_bin'))
    short_name = Column(TEXT(collation='utf8_bin'))
    activity_type = Column(VARCHAR(3, collation='utf8_bin'))
    end_of_participation = Column(BOOLEAN)
    ec_contribution = Column(FLOAT)
    country = Column(VARCHAR(2))
    street = Column(TEXT(collation='utf8_bin'))
    city = Column(TEXT(collation='utf8_bin'))
    post_code = Column(VARCHAR(20, collation='utf8_bin'))
    organization_url = Column(TEXT(collation='utf8_bin'))
    vat_number = Column(VARCHAR(20, collation='utf8_bin'))
    contact_form = Column(TEXT(collation='utf8_bin'))
    contact_type = Column(VARCHAR(14, collation='utf8_bin'))
    contact_title = Column(VARCHAR(5, collation='utf8_bin'))
    contact_first_names = Column(VARCHAR(12, collation='utf8_bin'))
    contact_last_names = Column(VARCHAR(14, collation='utf8_bin'))
    contact_telephone_number = Column(VARCHAR(17, collation='utf8_bin'))
    contact_fax_number = Column(VARCHAR(17, collation='utf8_bin'))

class ProjectPublications(Base):
    __tablename__ = 'cordisH2020_project_publications'
    rcn = Column(INTEGER, index=True)
    title = Column(TEXT(collation='utf8_bin'))
    #project_id = Column(INTEGER)
    #project_acronym = Column(TEXT(collation='utf8_bin'))
    #programme = Column(TEXT(collation='utf8_bin'))
    #topics = Column(VARCHAR(37))
    authors = Column(TEXT(collation='utf8_bin'))
    journal_title = Column(TEXT(collation='utf8_bin'))
    journal_number = Column(TEXT(collation='utf8_bin'))
    published_year = Column(INTEGER)
    published_pages = Column(TEXT(collation='utf8_bin'))
    issn = Column(VARCHAR(9), primary_key=True)
    doi = Column(TEXT(collation='utf8_bin'))
    is_published_as  = Column(VARCHAR(26))
    last_update_date = Column(DATETIME)

class Projects(Base):
    __tablename__ = 'cordisH2020_projects'
    rcn = Column(INTEGER, primary_key=True, autoincrement=False)
    id = Column(INTEGER)
    acronym = Column(TEXT(collation='utf8_bin'))
    status = Column(VARCHAR(10, collation='utf8_bin'))
    ##programme = Column(TEXT(collation='utf8_bin'))
    ##framework_programme = Column(VARCHAR(5))
    programmes = relationship('Programme', 
                              secondary='cordisH2020_project_programmes')
    topics = Column(VARCHAR(37, collation='utf8_bin'))
    title = Column(TEXT(collation='utf8_bin'))
    start_date = Column(DATETIME)
    end_date = Column(DATETIME)
    project_url = Column(TEXT(collation='utf8_bin'))
    objective = Column(TEXT(collation='utf8_bin'))
    total_cost = Column(FLOAT)
    ec_max_contribution = Column(FLOAT)
    call = Column(TEXT(collation='utf8_bin'))
    funding_scheme = Column(VARCHAR(14, collation='utf8_bin'))
    #coordinator = Column(TEXT(collation='utf8_bin'))
    #coordinator_country = Column(VARCHAR(2))
    #participants = Column(TEXT(collation='utf8_bin'))
    #participant_countries = Column(TEXT(collation='utf8_bin'))

class Reports(Base):
    __tablename__ = 'cordisH2020_reports'
    rcn = Column(INTEGER, primary_key=True)
    #language = Column(TEXT(collation='utf8_bin')) ## BAD
    title = Column(TEXT(collation='utf8_bin'))
    teaser = Column(TEXT(collation='utf8_bin'))
    summary = Column(TEXT(collation='utf8_bin'))
    work_performed = Column(TEXT(collation='utf8_bin'))
    final_results = Column(TEXT(collation='utf8_bin'))
    last_update_date = Column(DATETIME, primary_key=True)
    #project_id = Column(TEXT(collation='utf8_bin'))
    #project_acronym = Column(TEXT(collation='utf8_bin'))
    #programme = Column(TEXT(collation='utf8_bin'))
    #topics = Column(TEXT(collation='utf8_bin'))
    related_file = Column(TEXT(collation='utf8_bin'))
    url = Column(TEXT(collation='utf8_bin'))

class ProjectDeliverables(Base):
    __tablename__ = 'cordisH2020_project_deliverables'
    rcn = Column(INTEGER, primary_key=True)
    title = Column(TEXT(collation='utf8_bin'))
    #project_id = Column(INTEGER)
    #project_acronym = Column(TEXT(collation='utf8_bin'))
    #programme = Column(TEXT(collation='utf8_bin'))
    #topics = Column(TEXT(collation='utf8_bin'))
    description = Column(TEXT(collation='utf8_bin'))
    deliverable_type = Column(TEXT(collation='utf8_bin'))
    url = Column(TEXT(collation='utf8_bin'))
    last_update_date = Column(DATETIME, primary_key=True)
