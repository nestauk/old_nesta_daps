def hashable_tokens(name):
    _name = set(name.split(" ")) # unique tokens
    if '' in _name:
        _name.remove('') # ignore empty tokens
    return tuple(sorted(_name))  # standardise order, and make hashable


def process_name(name):
    translator = str.maketrans(string.punctuation, ' '*len(string.punctuation)) #map punctuation to space
    _name = name.translate(translator)  # remove punctuation
    _name = _name.lower()  # lowercase
    return hashable_tokens(_name)


def _evaluate_matches(match_scores, score_threshold, multinat_threshold, 
                      multimatch_threshold, long_name_threshold,
                      cb_ctry, reverse_lookup, grid_ctry_lookup, disputed_ctrys):

    # Generate other form (if any) of this country's id
    other_ctry = disputed_ctrys[cb_ctry] if cb_ctry in disputed_ctrys else cb_ctry
    
    # Find a match
    found_gids = set()
    scores = Counter(match_scores).most_common()  # best scores first
    for name, score in scores:
        if score < score_threshold:
            continue
        gids = reverse_lookup[name]
        
        # Generate set of GRID countries for these ids
        grid_ctrys = set(grid_ctry_lookup[gid] for gid in gids)        
        _disputed_ctrys = set()
        for ctry in grid_ctrys:
            if ctry not in disputed_ctrys:
                continue
            _disputed_ctrys.add(disputed_ctrys[ctry])
        grid_ctrys = grid_ctrys.union(_disputed_ctrys)
            
        # Country-based matching criteria
        no_cb_ctry = (cb_ctry is None or grid_ctrys == {None}) and score == 1  # the softest criteria: consider removing
        ctry_match = (cb_ctry in grid_ctrys or other_ctry in grid_ctrys) # good criteria
        # Circumstantial matching
        is_very_multinational =  (len(grid_ctrys) >= 3) # it's difficult to exclude matches on this basis
        multiple_matches = len(matches) > 1  # very strong criteria, regardless of country matching
        is_long_name = any(len(name) >= 4 for name in matches) and score == 1  # chance of accidentally matching a very long name seem slim, unless fuzzy
        
        #print(no_cb_ctry, ctry_match, is_very_multinational, multiple_matches, is_long_name)
        
        if not any((no_cb_ctry, ctry_match, is_very_multinational, multiple_matches, is_long_name)):
            continue
        score_threshold = score # Only take future scores if they're at least this good
        found_gids = found_gids.union(gids)
    if len(found_gids) == 0:
        score_threshold = None
    return found_gids, score_threshold

class MatchEvaluator:
    """Match based on scoring """
    def __init__(self, reverse_lookup, grid_ctry_lookup, disputed_ctrys,
                 score_threshold=0.8, multinat_threshold=3, 
                 multimatch_threshold=1, long_name_threshold=4):
        self.score_threshold = score_threshold
        self.multinat_threshold = multinat_threshold
        self.multimatch_threshold = multimatch_threshold
        self.long_name_threshold = long_name_threshold
        self.reverse_lookup = reverse_lookup
        self.grid_ctry_lookup = grid_ctry_lookup
        self.disputed_ctrys = disputed_ctrys
    
    def evaluate_matches(self, cb_ctry, match_scores):
        return _find_matches(match_scores, self.score_threshold, 
                             self.multinat_threshold, self.multimatch_threshold, 
                             self.long_name_threshold, cb_ctry, 
                             self.reverse_lookup, self.grid_ctry_lookup, self.disputed_ctrys)

        
    def generate_matches(self, data):
        matches, remaining_names = find_exact_matches(data)
        jaccard_matches = fast_nested_jaccard(remaining_names, self.all_grid_names)
        inexact_matches = find_inexact_matches(data, matches, jaccard_matches)
        matches.update(inexact_matches)
        return matches


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
