'''
Cordis
======
'''

from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.types import DATETIME, INTEGER, JSON
from sqlalchemy.types import TEXT, VARCHAR

Base = declarative_base()


class Project(Base):
    __tablename__ = 'cordis_projects'
    __table_args__ = {'mysql_collate': 'utf8_bin'}
    rcn = Column(INTEGER, primary_key=True, autoincrement=False)
    acronym = Column(TEXT)
    end_date_code = Column(DATETIME)
    ec_contribution = Column(INTEGER)
    framework = Column(VARCHAR(5), index=True)
    funding_scheme = Column(TEXT)
    funded_under = Column(JSON)
    objective =  Column(TEXT)
    project_description = Column(TEXT)
    start_date_code = Column(DATETIME, index=True)
    status = Column(VARCHAR(7))
    title = Column(TEXT)
    total_cost = Column(INTEGER)
    website = Column(TEXT)
    organizations = relationship('ProjectOrganisation')
    reports = relationship('Report')
    publications = relationship('Publication')
    topics = relationship('ProjectTopic')
    proposal_calls = relationship('ProjectProposalCall')


class Organisation(Base):
    __tablename__ = 'cordis_organisations'
    __table_args__ = {'mysql_collate': 'utf8_bin'}
    id = Column(INTEGER, primary_key=True, autoincrement=False)
    name = Column(TEXT)
    country_code = Column(VARCHAR(2), index=True)
    country_name = Column(TEXT)


class ProjectOrganisation(Base):
    __tablename__ = 'cordis_project_organisations'
    __table_args__ = {'mysql_collate': 'utf8_bin'}
    project_rcn = Column(INTEGER, ForeignKey(Project.rcn),
                         primary_key=True)
    organization_id = Column(INTEGER, ForeignKey(Organisation.id),
                             primary_key=True,
                             autoincrement=False)
    organization = relationship('Organisation')
    activity_type = Column(TEXT)
    address = Column(JSON)
    contribution = Column(INTEGER)
    type = Column(TEXT)
    website = Column(TEXT)


class Report(Base):
    __tablename__ = 'cordis_reports'
    __table_args__ = {'mysql_collate': 'utf8_bin'}
    rcn = Column(INTEGER, primary_key=True, autoincrement=False)
    project_rcn = Column(INTEGER, ForeignKey(Project.rcn),
                         primary_key=True)
    final_results = Column(TEXT)
    work_performed = Column(TEXT)
    teaser = Column(TEXT)
    summary = Column(MEDIUMTEXT)
    title = Column(TEXT)


class Publication(Base):
    __tablename__ = 'cordis_publications'
    __table_args__ = {'mysql_collate': 'utf8_bin'}
    id = Column(VARCHAR(255), primary_key=True)
    project_rcn = Column(INTEGER, ForeignKey(Project.rcn),
                         index=True)
    authors = Column(JSON)
    url = Column(TEXT)
    pid = Column(JSON)
    publisher = Column(TEXT)
    title = Column(TEXT)


class Topic(Base):
    __tablename__ = 'cordis_topics'
    __table_args__ = {'mysql_collate': 'utf8_bin'}
    rcn = Column(INTEGER, primary_key=True, autoincrement=False)
    title = Column(TEXT)


class ProjectTopic(Base):
    """Association table to CorEx topics."""
    __tablename__ = 'cordis_project_topics'
    project_rcn = Column(INTEGER, ForeignKey(Project.rcn),
                         primary_key=True)
    rcn = Column(INTEGER, ForeignKey(Topic.rcn),
                 primary_key=True,
                 autoincrement=False)
    topic = relationship('Topic')


class ProposalCall(Base):
    __tablename__ = 'cordis_proposal_calls'
    __table_args__ = {'mysql_collate': 'utf8_bin'}
    rcn = Column(INTEGER, primary_key=True, autoincrement=False)
    title = Column(TEXT)


class ProjectProposalCall(Base):
    __tablename__ = 'cordis_project_proposal_calls'
    project_rcn = Column(INTEGER, ForeignKey(Project.rcn),
                         primary_key=True)
    rcn = Column(INTEGER, ForeignKey(ProposalCall.rcn),
                 primary_key=True,
                 autoincrement=False)
    proposal_call = relationship('ProposalCall')
