'''
CORDIS
======
'''

from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR, TEXT
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import DATE, INTEGER, FLOAT, BOOLEAN, DATETIME

Base = declarative_base()

"""Association table for Projects and Subjects."""
project_subjects = Table('cordis_fp7_project_subjects', Base.metadata,
                         Column('project_rcn',
                                INTEGER,
                                ForeignKey('cordis_fp7_projects.rcn'),
                                primary_key=True),
                         Column('subject_code',
                                VARCHAR(3),
                                ForeignKey('cordis_subjects.code'),
                                primary_key=True))


class Project(Base):
    """FP7 projects."""
    __tablename__ = 'cordis_fp7_projects'

    rcn = Column(INTEGER, primary_key=True, autoincrement=False)
    id = Column(INTEGER)
    acronym = Column(VARCHAR(40))
    status = Column(VARCHAR(3))
    programme_code = Column(VARCHAR(40), ForeignKey('cordis_fp7_programmes.code'))
    programme = relationship("Programme")  # links to Programme m-o
    topics = Column(VARCHAR(100))  # lookup table unknown
    framework_programme = Column(VARCHAR(3))
    title = Column(TEXT)
    start_date = Column(DATE)
    end_date = Column(DATE)
    project_url = Column(TEXT)
    objective = Column(TEXT(collation='utf8_bin'))
    total_cost = Column(FLOAT)
    ec_max_contribution = Column(FLOAT)
    call = Column(VARCHAR(50))
    funding_scheme_code = Column(VARCHAR(20), ForeignKey('cordis_funding_schemes.code'))
    _funding_scheme = relationship("FundingScheme")  # links to FundingScheme m-o
    funding_schemes = association_proxy('_funding_scheme', 'title')
    # coordinator = Column(TEXT, collation='utf8_bin')
    # participants = Column(TEXT, collation='utf8_bin')
    subjects = relationship("Subject", secondary=project_subjects)  # links to subjects m-m
    organisations = relationship("Organization")
    reports = relationship("ReportSummary", uselist=False,
                           back_populates='project')


class Programme(Base):
    """FP7 programmes."""
    __tablename__ = 'cordis_fp7_programmes'

    rcn = Column(INTEGER)
    code = Column(VARCHAR(40), primary_key=True, autoincrement=False)
    title = Column(VARCHAR(300, collation='utf8_bin'))
    short_title = Column(VARCHAR(100, collation='utf8_bin'))
    language = Column(VARCHAR(2), primary_key=True, autoincrement=False)


class FundingScheme(Base):
    """Cordis funding schemes."""
    __tablename__ = 'cordis_funding_schemes'

    code = Column(VARCHAR(20), primary_key=True, autoincrement=False)
    title = Column(VARCHAR(150))
    # description is all empty
    # language is always en


class Subject(Base):
    """Cordis subjects."""
    __tablename__ = 'cordis_subjects'

    code = Column(VARCHAR(3), primary_key=True, autoincrement=False)
    title = Column(VARCHAR(100, collation='utf8_bin'))
    description = Column(VARCHAR(300, collation='utf8_bin'))
    language = Column(VARCHAR(2), primary_key=True, autoincrement=False)


class ReportSummary(Base):
    __tablename__ = 'cordis_fp7_report_summaries'

    rcn = Column(INTEGER, ForeignKey('cordis_fp7_projects.rcn'), primary_key=True)
    project = relationship('Project', back_populates='reports')
    # language = Column(VARCHAR())  # all are en
    title = Column(TEXT)
    teaser = Column(TEXT)
    # summary = Column(VARCHAR(3))  # all empty
    # workPerformed = Column(VARCHAR(3))  # all empty
    # finalResults = Column(VARCHAR(3))  # all empty
    lastUpdateDate = Column(DATETIME)
    # country = Column(VARCHAR())
    # projectID = Column(VARCHAR())
    # projectAcronym = Column(VARCHAR())
    # programme = Column(VARCHAR())
    # topics = Column(VARCHAR())
    relatedFile = Column(TEXT)
    url = Column(TEXT)
    article = Column(TEXT)


class Organization(Base):
    __tablename__ = 'cordis_fp7_organizations'

    project_rcn = Column(INTEGER, ForeignKey('cordis_fp7_projects.rcn'), index=True)
    # project_id = Column(INTEGER)
    # project_acronym = Column(TEXT)
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
    # contact_function
    contact_telephone_number = Column(VARCHAR(17))
    contact_fax_number = Column(VARCHAR(17))
