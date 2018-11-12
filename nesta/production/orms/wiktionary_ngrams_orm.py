'''
Wiktionary N-Grams
==================

N-grams extracted from Wiktionary.
'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, VARCHAR

Base = declarative_base()


class WiktionaryNgram(Base):
    __tablename__ = 'wiktionary_ngrams'
    ngram = Column(VARCHAR(50), primary_key=True)
