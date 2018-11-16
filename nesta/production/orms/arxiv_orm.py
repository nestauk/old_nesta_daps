'''
Arxiv
======
'''
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import VARCHAR, BIGINT, TEXT, DECIMAL
from sqlalchemy.types import JSON, INT, TIMESTAMP
from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import relationship

Base = declarative_base()


class Article(Base):
    __tablename__ = 'arxiv_articles'

    id = Column(VARCHAR(20), primary_key=True, autoincrement=False)
    created = Column()
    updated = Column()
    title = Column(TEXT)
    categories = Column(VARCHAR(200))
    journal_ref = Column(VARCHAR(200))
    doi = Column(VARCHAR(200))
    msc_class = Column(VARCHAR(200))
    abstract = Column(TEXT)
    authors = Column(JSON)


class Msc(Base):
    __tablename__ = 'msc_codes'

    id = Column(VARCHAR(200), primary_key=True)
    description = Column(VARCHAR(200))


class Categories(Base):
    __tablename__ = 'arxiv_categories'

    id = Column(VARCHAR(200))
    description = Column(VARCHAR(200))
