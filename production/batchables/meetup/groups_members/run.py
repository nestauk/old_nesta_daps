import logging
from groups_members import get_all_members
from orms.orm_utils import get_mysql_engine
from orms.meetup_orm import Base
from orms.meetup_orm import Group
from orms.meetup_orm import GroupMember
from sqlalchemy import and_

def run():
    logging.getLogger().setLevel(logging.INFO)

    # Load connection to the input db, and create the tables
    engine = get_mysql_engine("BATCHPAR_dbconf", "mysqldb")
    Base.metadata.create_all(engine)
    Session = sessionmaker(engine)
    session = Session()

    # 
    condition = Group.id == os.environ["BATCHPAR_groupid"]
    groups = session.query(Group).filter(condition).all()

    # Collect group info
    groups = set((g.id, g.urlname) for row in groups)
    logging.info("Got %s distinct groups from database", len(groups))

    # Collect members
    output = []
    for group_id, group_urlname in groups:
        logging.info("Getting %s", group_urlname)
        members = get_all_members(group_id, group_urlname, max_results=200)
        output += members
    logging.info("Got %s members", len(output))

    # Write the output
    outrows = [GroupMember(**row) for row in output]
    session.add_all(outrows)
    session.commit()
    session.close()
