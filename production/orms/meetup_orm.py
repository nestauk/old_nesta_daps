'''
Meetup
======


'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import VARCHAR, BIGINT, TEXT, DECIMAL
from sqlalchemy.types import JSON, INT, TIMESTAMP
from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import relationship

Base = declarative_base()

class Group(Base):
    __tablename__ = 'meetup_groups'

    id = Column(BIGINT(20), primary_key=True, autoincrement=False)
    urlname = Column(VARCHAR(200), unique=True)
    category_name = Column(VARCHAR(50))
    category_shortname = Column(VARCHAR(50))
    city = Column(VARCHAR(100))
    country = Column(VARCHAR(100))
    created = Column(BIGINT(20))
    description = Column(TEXT)
    lat = Column(DECIMAL(5,2))
    lon = Column(DECIMAL(5,2))
    members = Column(INT)
    name = Column(TEXT)
#    topics = Column(JSON)
    category_id = Column(INT)
    country_name = Column(VARCHAR(100))
    timestamp = Column(TIMESTAMP)

#    meetup_groups_members = relationship("GroupMember", 
#                                         cascade="all", 
#                                         backref="meetup_groups")


class GroupMember(Base):
    __tablename__ = 'meetup_groups_members'

    group_id = Column(BIGINT(20), 
                      ForeignKey("meetup_groups.id"),
                      primary_key=True)
    group_urlname = Column(VARCHAR(200),                            
                           ForeignKey("meetup_groups.urlname"))
    member_id = Column(BIGINT(20), primary_key=True)

 #   meetup_groups = relationship("Group", 
 #                                backref="meetup_groups_members")

