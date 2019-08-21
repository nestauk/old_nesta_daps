'''
ONS NOMIS ORM
=======================

The model for ONS's NOMIS data.

'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import VARCHAR, TEXT
from sqlalchemy.types import INT, DATETIME, FLOAT
from sqlalchemy import Column

Base = declarative_base()

class NomisGeographyLookup(Base):
    __tablename__ = 'nomis_geography_lookup'

    code = Column(VARCHAR(9), primary_key=True)
    name = Column(TEXT)


class NomisIndustryLookup(Base):
    __tablename__ = 'nomis_industry_lookup'

    code = Column(VARCHAR(4), primary_key=True)
    name = Column(TEXT)


class NomisEmploymentSizebandLookup(Base):
    __tablename__ = 'nomis_employment_sizeband_lookup'

    code = Column(INT, primary_key=True, autoincrement=False)
    name = Column(TEXT)


class NomisBusinessCount(Base):
    __tablename__ = 'nomis_businesscount'

    date = Column(DATETIME, primary_key=True, index=True)
    employment_sizeband_code = Column(INT, primary_key=True, index=True, autoincrement=False)
    geography_code = Column(VARCHAR(9), primary_key=True, index=True)
    industry_code = Column(VARCHAR(4), primary_key=True, index=True)
    obs_value = Column(FLOAT)


class NomisVariableLookup(Base):
    __tablename__ = 'nomis_variable_lookup'

    code = Column(INT, primary_key=True, autoincrement=False)
    name = Column(TEXT)


class NomisEmployment(Base):
    __tablename__ = 'nomis_employment'

    date = Column(DATETIME, primary_key=True, index=True)
    geography_code = Column(VARCHAR(9), primary_key=True, index=True)
    obs_value = Column(FLOAT)
    variable_code = Column(INT, primary_key=True, index=True, autoincrement=False)


# class NomisPopulationCensus(Base):
#     __tablename__ = 'nomis_population_census'

#     date = Column(INT, primary_key=True, index=True)
#     indgepuk11_code = Column(VARCHAR(1), primary_key=True, index=True)
#     geography_code = Column(VARCHAR(9), primary_key=True, index=True)
#     indgepuk11_name = Column(VARCHAR(130))
#     obs_value = Column(INT)

class NomisSexLookup(Base):
    __tablename__ = 'nomis_sex_lookup'

    code = Column(INT, primary_key=True, autoincrement=False)
    name = Column(TEXT)


class NomisGenderLookup(Base):
    __tablename__ = 'nomis_gender_lookup'

    code = Column(INT, primary_key=True, autoincrement=False)
    name = Column(TEXT)


class NomisAgeLookup(Base):
    __tablename__ = 'nomis_c_age_lookup'

    code = Column(INT, primary_key=True, autoincrement=False)
    name = Column(TEXT)

class NomisOtherAgeLookup(Base):
    __tablename__ = 'nomis_age_lookup'

    code = Column(INT, primary_key=True, autoincrement=False)
    name = Column(TEXT)


class NomisPopulationEstimate(Base):
    __tablename__ = 'nomis_population_estimate'

    c_age_code = Column(INT, primary_key=True, index=True, autoincrement=False)
    date = Column(DATETIME, primary_key=True, index=True)
    gender_code = Column(INT, primary_key=True, index=True, autoincrement=False)
    geography_code = Column(VARCHAR(9), primary_key=True, index=True)
    obs_value = Column(FLOAT)


class NomisWorkforceJobs(Base):
    __tablename__ = 'nomis_workforce_jobs'

    date = Column(DATETIME, primary_key=True, index=True)
    geography_code = Column(VARCHAR(9), primary_key=True, index=True)
    industry_code = Column(VARCHAR(3), primary_key=True, index=True)
    sex_code = Column(INT, primary_key=True, index=True, autoincrement=False)
    obs_value = Column(FLOAT)

class NomisClaimantCount(Base):
    __tablename__ = "nomis_claimantcount"
    
    date = Column(DATETIME, primary_key=True, index=True)
    geography_code = Column(VARCHAR(9), primary_key=True, index=True)
    gender_code = Column(INT, primary_key=True, index=True, autoincrement=False)
    age_code = Column(INT, primary_key=True, index=True, autoincrement=False)
    obs_value = Column(FLOAT)

class NomisMedianWages(Base):
    __tablename__ = "nomis_median_wages"
    
    date = Column(DATETIME, primary_key=True, index=True)
    geography_code = Column(VARCHAR(9), primary_key=True, index=True)
    sex_code = Column(INT, primary_key=True, index=True, autoincrement=False)
    pay_code = Column(INT, primary_key=True, index=True, autoincrement=False)
    obs_value = Column(FLOAT)

class NomisPayLookup(Base):
    __tablename__ = "nomis_pay_lookup"
    
    code = Column(INT, primary_key=True, autoincrement=False)
    name = Column(TEXT)
