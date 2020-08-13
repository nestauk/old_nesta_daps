from nesta.packages.grid.grid_matching import process_name
from nesta.packages.grid.grid_matching import GridMatcher

class GridCBMatchingTask(luigi.Task):
    
    def run(self):
        engine = get_mysql_engine("MYSQLDB", "mysqldb", "production") 
        name_fields = ['alias1', 'alias2', 'alias3', 'legal_name', 'name']
        cb_fields = ['id', 'country_code', 'parent_id'] + name_fields
        chunks = pd.read_sql_table('crunchbase_organizations', engine, columns=cb_fields , chunksize=10000)
        cb_df = pd.concat(chunks)        
        cb_df['names'] = cb_df.apply(lambda row: {process_name(row[k]) for k in name_fields 
                                                  if k in row and row[k] is not None}, axis=1)
        cb_df["iso2_code"] = cb_df.country_code.apply(lambda iso3: alpha3_to_alpha2[iso3])
        cb_data = {row['id']: {"names":row["names"], "iso2_code": row["iso2_code"]}
                   for _, row in cb_df.iterrows()}
        del cb_df
        del chunks

        # Takes about 25 mins
        matcher = GridMatcher()
        matches = matcher.generate_matches(cb_data)
        
        # write to disk
        write_data(data=matches, Base=Base, etc=etc)
