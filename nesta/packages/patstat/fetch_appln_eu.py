from nesta.core.orms.orm_utils import get_mysql_engine
import os
from sqlalchemy.orm import Session
import pandas as pd


def generate_temp_tables(engine, limit=None):
    '''
    Generate some temporary tables, 
    which will be selected by Pandas
    '''
    if limit is None:
        limit = 18446744073709551615  # BIGINT

    path = os.path.abspath(__file__)
    dir_path = os.path.dirname(path)
    pysql_path = "patstat_eu.sql"
    data_path = os.path.join(dir_path, pysql_path)
    with open(data_path) as f:
        sql = f.read()
    session = Session(bind=engine)    
    for _sql in sql.split("\n\n\n"):
        session.execute(_sql, {"limit":limit})
    session.commit()
    return session


def temp_tables_to_dfs(engine, tables=["tmp_appln_fam_groups",
                                       "tmp_appln_no_fam"],
                       chunksize=10000, limit=None):
    '''Read the required temporary tables into Pandas'''
    sql_select = "SELECT * FROM %s"
    dfs = {}
    if limit is not None and chunksize > limit:
        chunksize = limit
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
    '''Split the given field, and pop it out of the input'''
    return str(data.pop(col)).split(delimiter)
    

def concat_dfs(dfs):
    '''
    Join both of the temporary tables 
    together (since they have the same schema)
    '''
    data = [{'appln_id': pop_and_split(row, 'appln_id'),
             'appln_auth': pop_and_split(row, 'appln_auth'),
             **row} for _, df in dfs.items()
            for _, row in df.iterrows()]
    return data


def extract_data(limit=None, db='patstat_2019_05_13'):
    '''
    '''
    engine = get_mysql_engine('MYSQLDB', 'mysqldb', db)
    session = generate_temp_tables(engine, limit=limit)
    dfs = temp_tables_to_dfs(engine, limit=limit)
    session.close()
    del session
    return concat_dfs(dfs)

if __name__ == "__main__":
    data = extract_data()
