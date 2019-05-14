'''
Arxiv
=====
'''
from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR, TEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import JSON, DATE, INTEGER, BIGINT, FLOAT

from nesta.production.orms.grid_orm import Institutes
from nesta.production.orms.grid_orm import Base as GridBase
from nesta.production.orms.mag_orm import FieldOfStudy
from nesta.production.orms.mag_orm import Base as MagBase
from nesta.production.orms.orm_utils import merge_metadata

Base = declarative_base()
# Merge metadata with MAG and GRID
merge_metadata(Base, MagBase, GridBase)


"""Association table for Arxiv articles and their categories."""
article_categories = Table('arxiv_article_categories', Base.metadata,
                           Column('article_id',
                                  VARCHAR(20),
                                  ForeignKey('arxiv_articles.id'),
                                  primary_key=True),
                           Column('category_id',
                                  VARCHAR(40),
                                  ForeignKey('arxiv_categories.id'),
                                  primary_key=True))


"""Association table to Microsoft Academic Graph fields of study."""
article_fields_of_study = Table('arxiv_article_fields_of_study', Base.metadata,
                                Column('article_id',
                                       VARCHAR(20),
                                       ForeignKey('arxiv_articles.id'),
                                       primary_key=True),
                                Column('fos_id',
                                       BIGINT,
                                       ForeignKey(FieldOfStudy.id),
                                       primary_key=True))


"""Association table to GRID institutes."""
article_institutes = Table('arxiv_article_institutes', Base.metadata,
                           Column('article_id',
                                  VARCHAR(20),
                                  ForeignKey('arxiv_articles.id'),
                                  primary_key=True),
                           Column('institute_id',
                                  VARCHAR(20),
                                  ForeignKey(Institutes.id),
                                  primary_key=True))


class Article(Base):
    """Arxiv articles and metadata."""
    __tablename__ = 'arxiv_articles'

    id = Column(VARCHAR(20), primary_key=True, autoincrement=False)
    datestamp = Column(DATE)
    created = Column(DATE)
    updated = Column(DATE)
    title = Column(TEXT)
    journal_ref = Column(TEXT)
    doi = Column(VARCHAR(200))
    abstract = Column(TEXT)
    authors = Column(JSON)
    mag_authors = Column(JSON)
    mag_id = Column(BIGINT)
    mag_match_prob = Column(FLOAT)
    citation_count = Column(INTEGER)
    citation_count_updated = Column(DATE)
    msc_class = Column(VARCHAR(200))
    categories = relationship('Category',
                              secondary=article_categories)
    fields_of_study = relationship(FieldOfStudy,
                                   secondary=article_fields_of_study)
    institutes = relationship(Institutes,
                              secondary=article_institutes)


class Category(Base):
    """Lookup table for Arxiv category descriptions."""
    __tablename__ = 'arxiv_categories'

    id = Column(VARCHAR(40), primary_key=True)
    description = Column(VARCHAR(100))


# to be added at a later date
# class ArticleMSC(Base):
#     """Association table for Arxiv articles to Mathematics Subject Classification."""
#     __tablename__ = 'arxiv_article_msc'

#     article_id = Column(VARCHAR(20), ForeignKey('arxiv_articles.id'), primary_key=True)
#     msc_id = Column(VARCHAR(40), primary_key=True)

# to be added at a later date
# class Msc(Base):
#     __tablename__ = 'msc_codes'

#     id = Column(VARCHAR(40), ForeignKey('arxiv_article_msc.msc_id'), primary_key=True)
#     description = Column(VARCHAR(100))
