'''
Impute Base ID
==============

What NiH don't tell you is that the `core_project_num` field
has a base project number, which is effectively the actual
core project number. This is essential for aggregating projects,
otherwise it will appear that many duplicates exist in the data.

This code here is for imputing these values using a simple regex of the form:

    {BASE_ID}-{an integer}-{an integer}-{an integer}

Any `core_project_num` failing this regex are ignored.
'''

import re
from sqlalchemy.orm import load_only

from nesta.core.orms.nih_orm import Projects
from nesta.core.orms.orm_utils import get_mysql_engine
from nesta.core.orms.orm_utils import db_session

REGEX = re.compile('^(.*)-(\d+)-(\d+)-(\d+)$')
def get_base_code(core_code):
    """Extract the base code from the core project number
    if the pattern matches, otherwise return the
    core project number."""
    try:
        core_code, _, _, _ = REGEX.findall(core_code)[0]
    except IndexError:
        pass
    return core_code


def impute_base_id(session, from_id, to_id):
    """Impute the base ID values back into the database.

    Args:
       session (sqlalchemy.Session): Active context-managed db session
       from_id (str): First NiH project PK to impute base ID for.
       to_id (str): Last NiH project PK to impute base ID for.
    """
    q = session.query(Projects)
    # Don't load bloaty fields, we don't need them
    q = q.options(load_only('application_id', 'core_project_num',
                            'base_core_project_num'))
    # Don't update cached objects, since we're not using them
    q = q.execution_options(synchronize_session=False)
    # Retrieve the projects
    q = q.filter(Projects.application_id.between(from_id, to_id))
    for project in q.all():
        # Ignore those which are null
        if project.core_project_num is None:
            continue
        # Extract the base code
        base_code = get_base_code(project.core_project_num)
        app_id = project.application_id
        # NB: the following triggers a SQL UPDATE when commit() is
        # called when the session context manager goes out of scope
        project.base_core_project_num = base_code


def retrieve_id_ranges(database, chunksize=1000):
    """Retrieve and calculate the input arguments,
    over which "impute_base_id" can be mapped"""
    engine = get_mysql_engine("MYSQLDB", "mysqldb", database)
    # First get all offset values
    with db_session(engine) as session:
        q = session.query(Projects.application_id)
        q = q.order_by(Projects.application_id)
        try:
            ids, = zip(*q.all())
        except ValueError:  # Forgiveness, if there are no IDs in the DB
            return []

    final_id = ids[-1]
    ids = list(ids[0::chunksize])  # Every {chunksize}th id
    # Pop the final ID back in, if it has been truncated
    if ids[-1] != final_id:
        ids.append(final_id)
    # Zip together consecutive pairs of arguments, i.e.
    # n-1 values of (from_id, to_id)
    # where from_id[n] == to_id[n-1]
    id_ranges = list(zip(ids, ids[1:]))
    return id_ranges


def impute_base_id_thread(from_id, to_id, database):
    """Apply "impute_base_id" over this chunk of IDs"""
    #from_id, to_id, database = args[0]  # Unpack thread args
    engine = get_mysql_engine("MYSQLDB", "mysqldb", database)
    with db_session(engine) as session:
        impute_base_id(session, from_id, to_id)
        # Note: Commit happens now
