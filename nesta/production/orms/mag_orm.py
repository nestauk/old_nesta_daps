'''
Microsoft Academic Graph
========================
'''
from sqlalchemy import Column
from sqlalchemy.dialects.mysql import VARCHAR, TEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import BIGINT, INTEGER

Base = declarative_base()


class FieldOfStudy(Base):
    """Fields of study."""
    __tablename__ = 'mag_fields_of_study'

    id = Column(BIGINT, primary_key=True, autoincrement=False)
    name = Column(VARCHAR(250, collation='utf8_bin'))
    level = Column(INTEGER)
    parent_ids = Column(TEXT)
    child_ids = Column(TEXT)
