from nesta.packages.grid.grid_matcher import process_name
from nesta.packages.grid.grid_matcher import MatchEvaluator
from nesta.core.orms.orm_utils import get_mysql_engine
from nesta.core.orms.orm_utils insert_data
from nesta.core.orms.grid_orm import Base, GridCrunchbaseLookup
import pandas as pd


def _process(row, name_fields):
    return {process_name(row[k]) for k in name_fields
            if k in row and row[k] is not None}

def read_cb_data(db):
    engine = get_mysql_engine("MYSQLDB", "mysqldb", db)
    name_fields = ['alias1', 'alias2', 'alias3', 'legal_name', 'name']
    cb_fields = ['id', 'country_code', 'parent_id'] + name_fields
    chunks = pd.read_sql_table('crunchbase_organizations', engine,
                               columns=cb_fields , chunksize=10000)
    df = pd.concat(chunks)
    df['names'] = df.apply(lambda r: _process(r, name_fields), axis=1)
    data = [{'id': row['id'], "names":row["names"],
             "iso3_code": row["country_code"]}
            for _, row in df.iterrows()]
    return data


class GridCBMatchingTask(luigi.Task):
    
    def output(self):
        

    def run(self):
        db = "production" is self.production else "test"
        cb_data = read_cb_data(db=db)
        if not self.production:
            cb_data = cb_data[:1000]
        # Takes about 25 mins
        matcher = MatchEvaluator()
        matches = matcher.generate_matches(cb_data)
        # Flatten ready to save to disk
        out_data = [{"crunchbase_id": k,
                     "grid_id": row["id"],
                     "matching_score": row['score']}
                    for k, row in matches.items()]
        # write to disk
        insert_data("MYSQLDB", "mysqldb", db, Base,
                    GridCrunchbaseLookup, out_data, low_memory=True)
