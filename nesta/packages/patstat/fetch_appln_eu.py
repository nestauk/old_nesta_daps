from nesta.core.orms.orm_utils import get_mysql_engine
import os
from sqlalchemy.orm import Session
import pandas as pd


def generate_temp_tables(engine):
    '''Generate some temporary tables, which will be selected by Pandas'''
    path = os.path.abspath(__file__)
    dir_path = os.path.dirname(path)
    pysql_path = "patstat_eu.sql"
    data_path = os.path.join(dir_path, pysql_path)
    with open(data_path) as f:
        sql = f.read()
    session = Session(bind=engine)
    for _sql in sql.split("\n\n"):
        session.execute(_sql)
    session.commit()
    return session


def temp_tables_to_dfs(engine):
    '''Read the required temporary tables into Pandas'''
    sql_select = "SELECT * FROM %s"
    tbls = ["tmp_appln_fam_groups", "tmp_appln_no_fam"]
    dfs = {tbl: pd.read_sql(sql_select % tbl, engine)
           for tbl in tbls}
    return dfs

def concat_dfs(dfs):
    data = [{'appln_id': row.pop('appln_id').split(','), 
             'appln_auth': row.pop('appln_auth').split(','),
             **row} for _, df in dfs.items()          
            for _, row in df.iterrows()] 
    return data


def extract_data(db='patstat_2019_05_13'):
    engine = get_mysql_engine('MYSQLDB', 'mysqldb', db)
    session = generate_temp_tables(engine)
    dfs = temp_tables_to_dfs(engine)
    session.close()
    del session
    return concat_dfs(dfs)

if __name__ == "__main__":
    dfs = extract_data()


    for k, df in dfs.items():
        print(k, len(df))
        print(df.head())
        print()
