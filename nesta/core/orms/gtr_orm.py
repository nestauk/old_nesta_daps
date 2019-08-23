'''
Gateway to research ORM
=======================

The model for Gateway to Research data, where variable
sizes have been determined from an IPython hack session,
sampling the first 100 projects.
'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import VARCHAR, TEXT, DECIMAL
from sqlalchemy.types import JSON, INT, DATETIME, FLOAT
from sqlalchemy import Column

Base = declarative_base()


class Projects(Base):
    """The spine of the dataset, connected to all other objects via LinkTable"""
    __tablename__ = "gtr_projects"

    id = Column(VARCHAR(36), primary_key=True)
    end = Column(DATETIME, index=True)
    title = Column(VARCHAR(200))
    status = Column(VARCHAR(30))
    grantCategory = Column(VARCHAR(50))
    leadFunder = Column(VARCHAR(50))
    abstractText = Column(TEXT)
    start = Column(DATETIME, index=True)
    created = Column(DATETIME)
    leadOrganisationDepartment = Column(VARCHAR(40))
    potentialImpact = Column(TEXT)
    techAbstractText = Column(TEXT)


class LinkTable(Base):
    """This is a "manufactured" table (i.e. doesn't officially exist in GtR).
    It provides links to the Project spine."""
    __tablename__ = 'gtr_link_table'

    id = Column(VARCHAR(72), primary_key=True, index=True)
    project_id = Column(VARCHAR(36), primary_key=True, index=True)
    rel = Column(VARCHAR(50), index=True)
    table_name = Column(VARCHAR(50), index=True)


class Person(Base):
    __tablename__ = "gtr_persons"

    id = Column(VARCHAR(36), primary_key=True)
    firstName = Column(VARCHAR(50))
    otherNames = Column(VARCHAR(50))
    surname = Column(VARCHAR(50))


class Organisation(Base):
    __tablename__ = "gtr_organisations"

    id = Column(VARCHAR(36), primary_key=True)
    name = Column(VARCHAR(200))
    addresses = Column(JSON)


class Participant(Base):
    __tablename__ = "gtr_participant"
    
    id = Column(VARCHAR(72), primary_key=True)
    organisation_id = Column(VARCHAR(36), index=True)
    projectCost = Column(FLOAT)
    grantOffer = Column(FLOAT)


class OrganisationLocation(Base):
    """This table is not in the orginal data. It contains all organisations and location
    details where it has been possible to ascertain them."""
    __tablename__ = "gtr_organisations_locations"

    id = Column(VARCHAR(36), primary_key=True)
    country_name = Column(VARCHAR(200))
    country_alpha_2 = Column(VARCHAR(2))
    country_alpha_3 = Column(VARCHAR(3))
    country_numeric = Column(VARCHAR(3))
    continent = Column(VARCHAR(2))
    latitude = Column(DECIMAL(precision=8, scale=6))
    longitude = Column(DECIMAL(precision=9, scale=6))


class Fund(Base):
    __tablename__ = "gtr_funds"

    id = Column(VARCHAR(36), primary_key=True)
    end = Column(DATETIME, index=True)
    start = Column(DATETIME, index=True)
    category = Column(VARCHAR(50))
    amount = Column(INT)
    currencyCode = Column(VARCHAR(3))


class KeyFinding(Base):
    __tablename__ = "gtr_outcomes_keyfindings"

    id = Column(VARCHAR(36), primary_key=True)
    description = Column(TEXT)
    exploitationPathways = Column(TEXT)
    sectors = Column(TEXT)
    supportingUrl = Column(VARCHAR(200))


class Topic(Base):
    __tablename__ = "gtr_topic"

    id = Column(VARCHAR(36), primary_key=True)
    text = Column(VARCHAR(200), index=True)
    topic_type = Column(VARCHAR(30), index=True)


class Collaboration(Base):
    __tablename__ = "gtr_outcomes_collaborations"

    id = Column(VARCHAR(36), primary_key=True)
    description = Column(TEXT)
    parentOrganisation = Column(VARCHAR(200), index=True)
    principalInvestigatorContribution = Column(TEXT)
    partnerContribution = Column(TEXT)
    start = Column(DATETIME, index=True)
    sector = Column(TEXT)
    country = Column(VARCHAR(100), index=True)
    impact = Column(TEXT)
    childOrganisation = Column(VARCHAR(200), index=True)


class FurtherFunding(Base):
    __tablename__ = "gtr_outcomes_furtherfundings"

    id = Column(VARCHAR(36), primary_key=True)
    end = Column(DATETIME, index=True)
    description = Column(TEXT)
    organisation = Column(VARCHAR(200), index=True)
    fundingId = Column(VARCHAR(36), index=True)
    start = Column(DATETIME, index=True)
    amount = Column(INT)
    currencyCode = Column(VARCHAR(3))
    sector = Column(TEXT)
    country = Column(VARCHAR(100), index=True)
    department = Column(VARCHAR(100))


class ImpactSummaries(Base):
    __tablename__ = "gtr_outcomes_impactsummaries"

    id = Column(VARCHAR(36), primary_key=True)
    description = Column(TEXT)
    impactTypes = Column(VARCHAR(100))
    sector = Column(TEXT)
    firstYearOfImpact = Column(INT)


class IntellectualProperty(Base):
    __tablename__ = "gtr_outcomes_intellectualproperties"

    id = Column(VARCHAR(36), primary_key=True)
    title = Column(VARCHAR(200))
    description = Column(TEXT)
    protection = Column(VARCHAR(50))
    patentId = Column(VARCHAR(50))
    yearProtectionGranted = Column(INT)
    impact = Column(TEXT)
    licensed = Column(VARCHAR(3))


class Spinouts(Base):
    __tablename__ = "gtr_outcomes_spinouts"

    id = Column(VARCHAR(36), primary_key=True)
    description = Column(TEXT)
    companyName = Column(VARCHAR(200), index=True)
    impact = Column(TEXT)
    website = Column(TEXT)
    yearEstablished = Column(INT)


class Publications(Base):
    __tablename__ = "gtr_outcomes_publications"

    id = Column(VARCHAR(36), primary_key=True)
    title = Column(VARCHAR(200))
    type = Column(VARCHAR(200), index=True)
    journalTitle = Column(VARCHAR(200))
    datePublished = Column(DATETIME, index=True)
    publicationUrl = Column(TEXT)
    pubMedId = Column(INT)
    issn = Column(VARCHAR(30))
    volumeTitle = Column(VARCHAR(200))
    doi = Column(VARCHAR(200), index=True)
    issue = Column(VARCHAR(50))
    author = Column(VARCHAR(100))
    isbn = Column(VARCHAR(200))
    chapterTitle = Column(VARCHAR(200))
    pageReference = Column(VARCHAR(30))


class ResearchMaterial(Base):
    __tablename__ = "gtr_outcomes_researchmaterials"

    id = Column(VARCHAR(36), primary_key=True)
    title = Column(VARCHAR(200))
    description = Column(TEXT)
    type = Column(VARCHAR(200), index=True)
    impact = Column(TEXT)
    providedToOthers = Column(VARCHAR(30))
    yearFirstProvided = Column(INT)
    supportingUrl = Column(TEXT)


class Dissemination(Base):
    __tablename__ = "gtr_outcomes_disseminations"

    id = Column(VARCHAR(36), primary_key=True)
    description = Column(TEXT)
    form = Column(VARCHAR(200))
    primaryAudience = Column(VARCHAR(200))
    yearsOfDissemination = Column(VARCHAR(200))
    impact = Column(TEXT)
    partOfOfficialScheme = Column(VARCHAR(50))
    supportingUrl = Column(TEXT)
    geographicReach = Column(VARCHAR(200), index=True)
    typeOfPresentation = Column(VARCHAR(100))


class PolicyInfluence(Base):
    __tablename__ = "gtr_outcomes_policyinfluences"
    
    id = Column(VARCHAR(36), primary_key=True)
    influence = Column(TEXT)
    type = Column(VARCHAR(200), index=True)
    geographicReach = Column(VARCHAR(200), index=True)
    guidelineTitle = Column(VARCHAR(200))
    supportingUrl = Column(TEXT)
    impact = Column(TEXT)


class Product(Base):
    __tablename__ = "gtr_outcomes_products"
    
    id = Column(VARCHAR(36), primary_key=True)
    title = Column(VARCHAR(200))
    description = Column(TEXT)
    type = Column(VARCHAR(200), index=True)
    stage = Column(VARCHAR(50))
    status = Column(VARCHAR(50))
    yearDevelopmentCompleted = Column(INT)
    impact = Column(TEXT)
    supportingUrl = Column(TEXT)


class ArtisticAndCreativeProduct(Base):
    __tablename__ = "gtr_outcomes_artisticandcreativeproducts"

    id = Column(VARCHAR(36), primary_key=True)
    title = Column(VARCHAR(200))
    description = Column(TEXT)
    type = Column(VARCHAR(200), index=True)
    impact = Column(TEXT)
    yearFirstProvided = Column(INT)


class ResearchDatabaseModel(Base):
    __tablename__ = "gtr_outcomes_researchdatabaseandmodels"

    id = Column(VARCHAR(36), primary_key=True)
    title = Column(VARCHAR(200))
    description = Column(TEXT)
    type = Column(VARCHAR(200), index=True)
    impact = Column(TEXT)
    providedToOthers = Column(VARCHAR(30))
    yearFirstProvided = Column(INT)
    supportingUrl = Column(TEXT)


class SoftwareAndTechnicalProducts(Base):
    __tablename__ = "gtr_outcomes_softwareandtechnicalproducts"

    id = Column(VARCHAR(36), primary_key=True)
    title = Column(VARCHAR(200))
    description = Column(TEXT)
    type = Column(VARCHAR(200), index=True)
    impact = Column(TEXT)
    softwareOpenSourced = Column(VARCHAR(30))
    yearFirstProvided = Column(INT)
    supportingUrl = Column(TEXT)
