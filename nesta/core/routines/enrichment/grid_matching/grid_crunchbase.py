from nesta.packages.grid.grid_matching import process_name
from nesta.packages.grid.grid_matching import GridMatcher

class GridCBMatchingTask(luigi.Task):
    
    def run(self):
        matcher = GridMatcher()   #reverse_lookup, grid_ctry_lookup, disputed_ctrys  <--- these from lookup
        #                            # all grid names and ids held inside this thing

        engine = get_mysql_engine("MYSQLDB", "mysqldb", "production") 
        cb_fields = ['id', 'alias1', 'alias2', 'alias3', 'city', 'country_code', 'legal_name', 'name', 'parent_id']
        chunks = pd.read_sql_table('crunchbase_organizations', engine, columns=cb_fields , chunksize=10000)
        dfs = [df for df in chunks]
        cb_df = pd.concat(dfs)
        
        name_fields = ['alias1', 'alias2', 'alias3', 'legal_name', 'name']
        cb_df['names'] = cb_df.apply(lambda row: {process_name(row[k]) for k in name_fields 
                                                  if k in row and row[k] is not None}, axis=1)
        cb_df["iso2_code"] = cb_df.country_code.apply(lambda iso3: alpha3_to_alpha2[iso3])

        # convert cb_df to cb_names = {id: {"names":{}, "iso2_code":iso2}}
        cb_data = {row['id']: {"names":row["names"], "iso2_code": row["iso2_code"]}
                   for _, row in cb_df.iterrows()}
        del cb_df

        # Takes about 25 mins
        matches = matcher.generate_matches(cb_data)
        
        # write to disk
        write_data(data=matches, Base=Base, etc=etc)
        
# in jaccard:
def fast_nested_jaccard(terms_to_index, terms_to_query):
    terms_to_index = list(terms_to_index)
    search_index = SearchIndex(terms_to_index, similarity_threshold=0.5)
    jaccard_matches = defaultdict(dict)
    for query_term in terms_to_query:
        for idx, score in search_index.query(query_term):
            index_term = terms_to_index[idx]
            jaccard_matches[index_term][query_term] = nested_jaccard(index_term, query_term)
    return jaccard_matches

#in GridMatcher:
def find_exact_matches(data):
    all_grid_names = set().union(*self.grid_df.names)
    all_comparison_names = set().union(row['names'] for row in data.values())
    exact_matches = all_comparison_names.intersection(self.all_grid_names)

    matches = {}
    matched_names = set()
    for id, row in data.items():
        matched_names = row["names"].intersection(exact_matches)
        gids, best_score = matcher.find_matches(iso2_code=row['iso2_code'], 
                                                match_scores={m: 1 for m in matched_names})
        if best_score is not None:
            matches[id] = {'grid_ids': gids, 'score': best_score}
            matched_names = matched_names.union(row["names"])
    remaining_names = all_comparison_names - matched_names
    return matches, remaining_names

def find_inexact_matches(data, exact_matches, jaccard_matches):
    matches = {}
    for id, row in data.items():
        if id in exact_matches:
            continue
        _score, _gids = 0, []
        for name in row['names']:
            if name not in jaccard_matches:
                continue        
            match_scores = jaccard_matches[name]
            gids, best_score = matcher.find_matches(iso2_code=row['country_code'], 
                                                    match_scores=match_scores)
            if best_score is not None and best_score > _score:
                _score = best_score
                _gids = gids
        if _score > 0:        
            matches[id] = {'grid_ids': gids, 'score': best_score}
    return matches

def Matcher.matcher.generate_matches(data):
    matches, remaining_names = find_exact_matches(data)
    jaccard_matches = fast_nested_jaccard(remaining_names, self.all_grid_names)
    inexact_matches = find_inexact_matches(data, matches, jaccard_matches)
    matches.update(inexact_matches)
    return matches
