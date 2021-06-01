"""
Arxiv
=====
"""
from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR, TEXT, MEDIUMTEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import JSON, DATE, INTEGER, BIGINT, FLOAT, BOOLEAN

from nesta.core.orms.grid_orm import Institute
from nesta.core.orms.grid_orm import Base as GridBase
from nesta.core.orms.mag_orm import FieldOfStudy
from nesta.core.orms.mag_orm import Base as MagBase
from nesta.core.orms.orm_utils import merge_metadata

Base = declarative_base()
# Merge metadata with MAG and GRID
merge_metadata(Base, MagBase, GridBase)


"""Association table for Arxiv articles and their categories."""
article_categories = Table(
    "arxiv_article_categories",
    Base.metadata,
    Column(
        "article_id", VARCHAR(40), ForeignKey("arxiv_articles.id"), primary_key=True
    ),
    Column(
        "category_id", VARCHAR(40), ForeignKey("arxiv_categories.id"), primary_key=True
    ),
)


"""Association table to Microsoft Academic Graph fields of study."""
article_fields_of_study = Table(
    "arxiv_article_fields_of_study",
    Base.metadata,
    Column(
        "article_id", VARCHAR(40), ForeignKey("arxiv_articles.id"), primary_key=True
    ),
    Column("fos_id", BIGINT, ForeignKey(FieldOfStudy.id), primary_key=True),
)


class ArticleInstitute(Base):
    """Association table to GRID institutes."""

    __tablename__ = "arxiv_article_institutes"

    article_id = Column(VARCHAR(40), ForeignKey("arxiv_articles.id"), primary_key=True)
    institute_id = Column(VARCHAR(20), ForeignKey(Institute.id), primary_key=True)
    is_multinational = Column(BOOLEAN)
    matching_score = Column(FLOAT)
    institute = relationship(Institute)


class Article(Base):
    """Arxiv articles and metadata."""

    __tablename__ = "arxiv_articles"

    id = Column(VARCHAR(40), primary_key=True, autoincrement=False)
    datestamp = Column(DATE)
    created = Column(DATE)
    updated = Column(DATE)
    title = Column(TEXT(collation="utf8mb4_unicode_ci"))
    journal_ref = Column(TEXT(collation="utf8mb4_unicode_ci"))
    doi = Column(VARCHAR(200))
    abstract = Column(MEDIUMTEXT(collation="utf8mb4_unicode_ci"))
    authors = Column(JSON)
    mag_authors = Column(JSON)
    mag_id = Column(BIGINT)
    mag_match_prob = Column(FLOAT)
    citation_count = Column(INTEGER)
    citation_count_updated = Column(DATE)
    msc_class = Column(VARCHAR(200))
    institute_match_attempted = Column(BOOLEAN, default=False)
    categories = relationship("Category", secondary=article_categories)
    fields_of_study = relationship(FieldOfStudy, secondary=article_fields_of_study)
    institutes = relationship("ArticleInstitute")
    corex_topics = relationship("CorExTopic", secondary="arxiv_article_corex_topics")
    article_source = Column(VARCHAR(7), index=True, default=None)


class Category(Base):
    """Lookup table for Arxiv category descriptions."""

    __tablename__ = "arxiv_categories"

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


class CorExTopic(Base):
    """CorEx topics derived from arXiv data"""

    __tablename__ = "arxiv_corex_topics"
    id = Column(INTEGER, primary_key=True, autoincrement=False)
    terms = Column(JSON)


class ArticleTopic(Base):
    """Association table to CorEx topics."""

    __tablename__ = "arxiv_article_corex_topics"
    article_id = Column(VARCHAR(40), ForeignKey(Article.id), primary_key=True)
    topic_id = Column(
        INTEGER, ForeignKey(CorExTopic.id), primary_key=True, autoincrement=False
    )
    topic_weight = Column(FLOAT)


class ArticleVector(Base):
    """Document vectors for articles."""

    __tablename__ = "arxiv_vector"
    article_id = Column(VARCHAR(40), ForeignKey(Article.id), primary_key=True)
    vector = Column(JSON)


class ArticleCluster(Base):
    """Document clusters for articles."""

    __tablename__ = "arxiv_cluster"
    article_id = Column(VARCHAR(40), ForeignKey(Article.id), primary_key=True)
    clusters = Column(JSON)
