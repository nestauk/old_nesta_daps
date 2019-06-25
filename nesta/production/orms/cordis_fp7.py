'''
CORDIS
======
'''

from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR, TEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import JSON, DATE, INTEGER, BIGINT, FLOAT, BOOLEAN

Base = declarative_base()


class Project(Base):
    """FP7 projects."""
    __tablename__ = 'cordis_fp7_projects'

    rcn = Column(INTEGER)
    id = Column(INTEGER, primary_key=True, autoincrement=False)
    acronym = Column(VARCHAR(40))
    status = Column(VARCHAR(3))
    programme = relationship("FP7Programme")
    topics = Column(VARCHAR())  # lookup table unknown
    framework_programme = Column(VARCHAR(3))
    title = Column(VARCHAR(300))
    start_date = Column(DATE)
    end_date = Column(DATE)
    project_url = Column(VARCHAR(250))
    objective = Column(TEXT)
    total_cost = Column(FLOAT)
    ec_max_contribution = Column(FLOAT)
    call = Column(VARCHAR(50))
    funding_scheme = relationship("FundingScheme")
    coordinator = Column(VARCHAR(200))  # link to particpants
    participants = Column(VARCHAR(TEXT))  # link to particpants
    subjects = Column(VARCHAR(30))  # link to subjects


class FP7Programme(Base):
    """FP7 programmes."""
    __tablename__ = 'cordis_fp7_programmes'

    rcn = Column(VARCHAR(6))
    code = Column(VARCHAR(40), primary_key=True, autoincrement=False,
                  ForeignKey=Project.programme)
    title = Column(VARCHAR(300))
    short_title = Column(VARCHAR(100))
    language = Column(VARCHAR(2), primary_key=True, autoincrement=False)


class FundingScheme(Base):
    """Cordis funding schemes."""
    __tablename__ = 'cordis_funding_schemes'

    code = Column(VARCHAR(20), primary_key=True, autoincrement=False,
                  ForeignKey=Project.funding_scheme)
    title = Column(VARCHAR(150))
    # description is all empty
    # language is always en
