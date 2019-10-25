'''
Patstat EU data
===============
'''
from sqlalchemy import Table, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import JSON, DATE, INTEGER

Base = declarative_base()

class ApplnFamily(Base):
    """
    Granted applications, grouped by family, with any person associated with
    any application in the family having an address in the EU.
    """
    __tablename__ = 'patstat_appln_family_eu'

    docdb_family_id = Column(INTEGER, primary_key=True, autoincrement=False)
    appln_id = Column(JSON)
    nb_citing_docdb_fam = Column(INTEGER)
    earliest_filing_date = Column(DATE)
    earliest_filing_year = Column(INTEGER)
    appln_auth = Column(JSON)
