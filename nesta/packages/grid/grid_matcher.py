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
