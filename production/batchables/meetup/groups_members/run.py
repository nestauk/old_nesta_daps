from groups_members import get_members
from groups_members import get_all_members
import logging
from sqlalchemy import engine_from_config
from configparser import ConfigParser

def run():
    logging.getLogger().setLevel(logging.INFO)

    # Load connection to the input db
    conf = ConfigParser(os.environ["BATCHPAR_dbconf"])
    engine = engine_from_config(conf._sections["mysqldb"])
    cnx = engine.connect()

    cursor = cnx.cursor()
    query = ("SELECT name, age FROM muppets_input WHERE age <= %s")
    cursor.execute(query, (self.max_age, ))
    data = [dict(name=name, age=age+1) for name, age in cursor]

    groups = json.load(f)

    # Collect group info
    groups = set((row['id'], row['urlname']) for row in groups)
    logging.info("Got %s distinct groups from database", len(groups))

    # Collect members
    output = []
    for group_id, group_urlname in groups:
        logging.info("Getting %s", group_urlname)
        members = get_all_members(group_id, group_urlname, max_results=200)
        output += members
    logging.info("Got %s members", len(output))

    # Write the output


