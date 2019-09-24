"""
Microsoft Academic Graph
========================
"""
from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR, TEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship
from sqlalchemy.types import BIGINT, INTEGER, DATE


Base = declarative_base()


class FieldOfStudy(Base):
    """Fields of study."""
    __tablename__ = 'mag_fields_of_study'

    id = Column(BIGINT, primary_key=True, autoincrement=False)
    name = Column(VARCHAR(250, collation='utf8_bin'))
    level = Column(INTEGER)
    parent_ids = Column(TEXT)
    child_ids = Column(TEXT)


class PaperFieldsOfStudy(Base):
    __tablename__ = 'mag_paper_fields_of_study'

    paper_id = Column(BIGINT,
                      ForeignKey('mag_papers.id'),
                      primary_key=True,
                      autoincrement=False)
    field_of_study_id = Column(BIGINT,
                               ForeignKey('mag_fields_of_study.id'),
                               primary_key=True,
                               autoincrement=False)
    paper = relationship('Paper', back_populates='fields_of_study')
    field_of_study = relationship('FieldOfStudy')


class PaperLanguage(Base):
    __tablename__ = 'mag_paper_languages'

    paper_id = Column(BIGINT, ForeignKey('mag_papers.id'), primary_key=True)
    language = Column(VARCHAR(20), primary_key=True)


class Paper(Base):
    __tablename__ = 'mag_papers'

    id = Column(BIGINT, primary_key=True, autoincrement=False)
    title = Column(TEXT(collation='utf8_bin'))
    citation_count = Column(INTEGER)
    created_date = Column(DATE)
    doi = Column(VARCHAR(200))
    book_title = Column(VARCHAR(200))
    _languages = relationship('PaperLanguage')
    languages = association_proxy('_languages', 'language',
                                  creator=lambda l: PaperLanguage(language=l))
    fields_of_study = relationship('PaperFieldsOfStudy', back_populates='paper')
    authors = relationship('PaperAuthor', back_populates='paper')


class PaperAuthor(Base):
    __tablename__ = 'mag_paper_authors'

    paper_id = Column(BIGINT,
                      ForeignKey('mag_papers.id'),
                      primary_key=True,
                      autoincrement=False)
    author_id = Column(BIGINT,
                       ForeignKey('mag_authors.id'),
                       primary_key=True,
                       autoincrement=False)
    paper = relationship('Paper', back_populates='authors')
    author = relationship('Author', back_populates='papers')


class Author(Base):
    __tablename__ = 'mag_authors'

    id = Column(BIGINT, primary_key=True, autoincrement=False)
    name = Column(VARCHAR(100, collation='utf8_bin'))
    # the foreign key constraint and relationship to grid_institutes is omitted here
    # as the datasets could be collected at different times
    grid_id = Column(VARCHAR(20))
    papers = relationship('PaperAuthor', back_populates='author')
