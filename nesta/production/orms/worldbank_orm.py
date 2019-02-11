'''
Worldbank schema
=================

Schema for Worldbank sociodemographic data. Excuse the verbose
variable names, but the alternative would have been raw codes like
'NYGDPMKTPSAKD', so I opted for an auto-generated human-readable schema.
'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import INTEGER, DECIMAL
from sqlalchemy.types import FLOAT, VARCHAR
from sqlalchemy import Column

Base = declarative_base()


class WorldbankCountry(Base):
    __tablename__ = 'worldbank_countries'

    # Metadata
    id = Column(VARCHAR(3), primary_key=True)
    capitalCity = Column(VARCHAR(19))
    incomeLevel = Column(VARCHAR(19))
    iso2Code = Column(VARCHAR(2), index=True)
    latitude = Column(DECIMAL(6, 4), index=True)
    longitude = Column(DECIMAL(6, 4), index=True)
    year = Column(INTEGER, primary_key=True)
    lendingType = Column(VARCHAR(14))
    name = Column(VARCHAR(54))
    region = Column(VARCHAR(26), index=True)

    # Data
    age_dependency_ratio_pc_of_working_age_population = Column(FLOAT)
    barro_lee_percentage_of_population_age_25_with_no_education = Column(FLOAT)
    barro_lee_percentage_of_population_age_25_with_tertiary_schooling_completed_tertiary = Column(FLOAT)
    gini_index = Column(FLOAT)
    life_expectancy_at_birth_total_years = Column(FLOAT)
    mortality_rate_infant_per_1_000_live_births = Column(FLOAT)
    population_total = Column(FLOAT)
    poverty_headcount_ratio_at_national_poverty_line_pc_of_population = Column(FLOAT)
    rural_population_pc_of_total_population = Column(FLOAT)

