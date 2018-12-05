'''
Geographic data
======
'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import VARCHAR, DECIMAL
from sqlalchemy.types import BOOLEAN
from sqlalchemy import Column

Base = declarative_base()


class Geographic(Base):
    __tablename__ = 'geographic_data'

    id = Column(VARCHAR(400), primary_key=True)  # composite key of city & country
    city = Column(VARCHAR(200))
    country = Column(VARCHAR(200))
    country_alpha_2 = Column(VARCHAR(2))
    country_alpha_3 = Column(VARCHAR(3))
    country_numeric = Column(VARCHAR(3))
    continent = Column(VARCHAR(2))
    latitude = Column(DECIMAL)
    longitude = Column(DECIMAL)
    done = Column(BOOLEAN, default=False)
