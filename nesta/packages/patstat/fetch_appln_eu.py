from nesta.core.orms.orm_utils import get_mysql_engine
import os
from sqlalchemy.orm import Session
import pandas as pd

MYSQL_INTEGER_LIMIT = 18446744073709551615  # BIGINT

def generate_temp_tables(engine, limit=MYSQL_INTEGER_LIMIT, region='eu'):
    '''
    Generate some temporary tables,
    which will be selected by Pandas.

    Args:
        engine (sqlalchemy.Engine): SqlAlchemy connectable.
        limit (int): Maximum number of results to return.
        region (str): 'eu' (default) for EU region, or 'all' for everything.
    Returns:
        session (sqlalchemy.Session): SqlAlchemy session in which the temp tables exist.
    '''
    dir_path = os.path.dirname(os.path.abspath(__file__))  # path to this very module
    data_path = os.path.join(dir_path, f"patstat_{region}.sql")  # this SQL file exists right here
    # Get the SQL code
    with open(data_path) as f:
        sql = f.read()
    # Generate the temporary tables
    session = Session(bind=engine)
    for _sql in sql.split("\n\n\n"):  # By my own convention, tables are separated by \n\n\n
        session.execute(_sql, {"limit":limit})
    session.commit()
    return session


def temp_tables_to_dfs(engine, tables=["tmp_appln_fam_groups",
                                       "tmp_appln_no_fam"],
                       chunksize=10000, limit=None):
    '''Read the required temporary tables into Pandas.
    
    Args:
        engine (sqlalchemy.Engine): SqlAlchemy connectable.
        tables (list): Tables to extract.
        chunksize (int): Streaming chunksize.
        limit (int): Max results to return per table.
    Returns:
        dfs (list): A list of pd.DataFrame for each requested table.
    '''
    if limit is not None and chunksize > limit:
        chunksize = limit
    sql_select = "SELECT * FROM %s"
    dfs = {}
    for tbl in tables:
        _df = []
        totalsize = 0
        for chunk in pd.read_sql(sql_select % tbl, engine,
                                 chunksize=chunksize):
            totalsize += len(chunk)
            _df.append(chunk)
            if limit is not None and totalsize >= limit:
                break
        dfs[tbl] = pd.concat(_df)
    return dfs


def pop_and_split(data, col, delimiter=','):
    '''Split the given field, which has been popped out of the input dict'''
    return str(data.pop(col)).split(delimiter)


def concat_dfs(dfs):
    '''Join both of the temporary tables
    together (since they have the same schema).
    
    Args:
        dfs (list): A list of pd.DataFrame for each requested table.
    Returns:
        data (list): Joined list of dictionaries of the combined tables.
    '''
    data = [{'appln_id': pop_and_split(row, 'appln_id'),
             'appln_auth': pop_and_split(row, 'appln_auth'),
             **row} for _, df in dfs.items()
            for _, row in df.iterrows()]
    return data


def extract_data(limit=None, db='patstat_2019_05_13', region='eu'):
    '''Get all patents, grouped and aggregated by their doc families'''
    engine = get_mysql_engine('MYSQLDB', 'mysqldb', db)
    session = generate_temp_tables(engine, limit=limit, region=region)
    dfs = temp_tables_to_dfs(engine, limit=limit)
    session.close()
    del session
    return concat_dfs(dfs)
