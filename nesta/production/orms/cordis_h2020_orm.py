'''
CORDIS H2020
============
'''

from sqlalchemy import Table, Column, ForeignKey, Index, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import VARCHAR, TEXT
from sqlalchemy.types import DATETIME, INTEGER, FLOAT, BOOLEAN

Base = declarative_base()


class ProjectProgrammes(Base):
    __tablename__ = 'cordis_h2020_project_programmes'
    __table_args__ = {'mysql_collate': 'utf8_bin'}
    project_rcn = Column(INTEGER, 
                         ForeignKey('cordis_h2020_projects.rcn'), 
                         primary_key=True)
    programme_code = Column(VARCHAR(19),
                            ForeignKey('cordis_h2020_programmes.id'), 
                            primary_key=True)

class Programme(Base):
    __tablename__ = 'cordis_h2020_programmes'
    __table_args__ = {'mysql_collate': 'utf8_bin'}
    id = Column(VARCHAR(19), primary_key=True)
    framework_programme = Column(VARCHAR(5))

class Organizations(Base):
    __tablename__ = 'cordis_h2020_organizations'
    __table_args__ = {'mysql_collate': 'utf8_bin'}
    project_rcn = Column(INTEGER, ForeignKey('cordis_h2020_projects.rcn'), index=True)
    #project_id = Column(INTEGER)
    #project_acronym = Column(TEXT)
    role = Column(VARCHAR(15))
    id = Column(INTEGER, primary_key=True, autoincrement=False)
    name = Column(TEXT)
    short_name = Column(TEXT)
    activity_type = Column(VARCHAR(3))
    end_of_participation = Column(BOOLEAN)
    ec_contribution = Column(FLOAT)
    country = Column(VARCHAR(2))
    street = Column(TEXT)
    city = Column(TEXT)
    post_code = Column(VARCHAR(20))
    organization_url = Column(TEXT)
    vat_number = Column(VARCHAR(20))
    contact_form = Column(TEXT)
    contact_type = Column(VARCHAR(14))
    contact_title = Column(VARCHAR(5))
    contact_first_names = Column(VARCHAR(12))
    contact_last_names = Column(VARCHAR(14))
    contact_telephone_number = Column(VARCHAR(17))
    contact_fax_number = Column(VARCHAR(17))

class Projects(Base):
    __tablename__ = 'cordis_h2020_projects'
    __table_args__ = {'mysql_collate': 'utf8_bin'}
    rcn = Column(INTEGER, primary_key=True, autoincrement=False)
    id = Column(INTEGER)
    acronym = Column(TEXT)
    status = Column(VARCHAR(10))
    ##programme = Column(TEXT)
    ##framework_programme = Column(VARCHAR(5))
    programmes = relationship('Programme', 
                              secondary='cordis_h2020_project_programmes')
    topics = Column(VARCHAR(37))
    title = Column(TEXT)
    start_date = Column(DATETIME)
    end_date = Column(DATETIME)
    project_url = Column(TEXT)
    objective = Column(TEXT)
    total_cost = Column(FLOAT)
    ec_max_contribution = Column(FLOAT)
    call = Column(TEXT)
    funding_scheme = Column(VARCHAR(14))
    #coordinator = Column(TEXT)
    #coordinator_country = Column(VARCHAR(2))
    #participants = Column(TEXT)
    #participant_countries = Column(TEXT)

class ProjectPublications(Base):
    __tablename__ = 'cordis_h2020_project_publications'
    __table_args__ = (Index("title_idx", "title", 
                            mysql_length=50, unique=True),
                      {'mysql_collate': 'utf8_bin'})
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    rcn = Column(INTEGER, ForeignKey('cordis_h2020_projects.rcn'), index=True)
    title = Column(TEXT)
    #project_id = Column(INTEGER)
    #project_acronym = Column(TEXT)
    #programme = Column(TEXT)
    #topics = Column(VARCHAR(37))
    authors = Column(TEXT)
    journal_title = Column(TEXT)
    journal_number = Column(TEXT)
    published_year = Column(INTEGER, primary_key=True)
    published_pages = Column(TEXT)
    issn = Column(VARCHAR(9))
    doi = Column(TEXT)
    is_published_as  = Column(VARCHAR(26))
    last_update_date = Column(DATETIME, primary_key=True)

class Reports(Base):
    __tablename__ = 'cordis_h2020_reports'
    __table_args__ = (Index("title_idx", "title", 
                            mysql_length=50, unique=True),
                      {'mysql_collate': 'utf8_bin'})
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    rcn = Column(INTEGER, ForeignKey('cordis_h2020_projects.rcn'), index=True)
    #language = Column(TEXT) ## BAD
    title = Column(TEXT)
    teaser = Column(TEXT)
    summary = Column(TEXT)
    work_performed = Column(TEXT)
    final_results = Column(TEXT)
    last_update_date = Column(DATETIME)
    #project_id = Column(TEXT)
    #project_acronym = Column(TEXT)
    #programme = Column(TEXT)
    #topics = Column(TEXT)
    related_file = Column(TEXT)
    url = Column(TEXT)

class ProjectDeliverables(Base):
    __tablename__ = 'cordis_h2020_project_deliverables'
    __table_args__ = (Index("title_idx", "title", 
                            mysql_length=50, unique=True),
                      {'mysql_collate': 'utf8_bin'})
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    rcn = Column(INTEGER, ForeignKey('cordis_h2020_projects.rcn'), index=True)
    title = Column(TEXT)
    #project_id = Column(INTEGER)
    #project_acronym = Column(TEXT)
    #programme = Column(TEXT)
    #topics = Column(TEXT)
    description = Column(TEXT)
    deliverable_type = Column(TEXT)
    url = Column(TEXT)
    last_update_date = Column(DATETIME)
