'''
Dummy ORM for ESCO occupations and their respective skills,
plus the number of co-occurrences of the skills
======
'''

from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.types import DATETIME, INTEGER, JSON
from sqlalchemy.types import TEXT, VARCHAR

Base = declarative_base()

class Occupations(Base):
    __tablename__ = 'esco_occupations'
    __table_args__ = {'mysql_collate': 'utf8_bin'}
    id = Column(INTEGER, primary_key=True, autoincrement=False, unique=True)
    preferred_label = Column(VARCHAR(80))
    alt_labels = Column(TEXT)
    concept_type = Column(VARCHAR(10))
    concept_uri = Column(VARCHAR(74))
    isco_group = Column(INTEGER)
    description = Column(TEXT)

class Skills(Base):
    __tablename__ = 'esco_skills'
    __table_args__ = {'mysql_collate': 'utf8_bin'}
    id = Column(INTEGER, primary_key=True, autoincrement=False, unique=True)
    preferred_label = Column(VARCHAR(80))
    alt_labels = Column(TEXT)
    concept_type = Column(VARCHAR(24))
    concept_uri = Column(VARCHAR(69))
    skill_type = Column(VARCHAR(26))
    reuse_level = Column(VARCHAR(19))
    description = Column(TEXT)

class OccupationSkills(Base):
    __tablename__ = 'esco_occupation_skills'
    __table_args__ = {'mysql_collate': 'utf8_bin'}
    occupation_id = Column(INTEGER, ForeignKey(Occupations.id))
    skill_id = Column(INTEGER, ForeignKey(Skills.id))
    importance = Column(VARCHAR(9))

class SkillCooccurrences(Base):
    __tablename__ = 'esco_skill_cooccurrences'
    __table_args__ = {'mysql_collate': 'utf8_bin'}
    skill_1 = Column(INTEGER, ForeignKey(Skills.id))
    skill_2 = Column(INTEGER, ForeignKey(Skills.id))
    coocurrences = Column(INTEGER)
