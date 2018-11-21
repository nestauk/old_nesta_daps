'''
Arxiv
======
'''
from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR, TEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import JSON, DATE

Base = declarative_base()


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
    msc_class = Column(VARCHAR(200))
    categories = relationship('ArticleCategories')


class ArticleCategories(Base):
    """Association table for Arxiv articles and their categories."""
    __tablename__ = 'arxiv_article_categories'

    article_id = Column(VARCHAR(20), ForeignKey('arxiv_articles.id'), primary_key=True)
    category_id = Column(VARCHAR(40), ForeignKey('arxiv_categories.id'), primary_key=True)


class Categories(Base):
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
