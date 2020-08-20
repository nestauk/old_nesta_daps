import string

PUNCT = string.punctuation

def hashable_tokens(string_to_split):
    """Split string into unique tokens, sort and return as tuple,
    which is hashable.

    Args:
        string_to_split (str): string to split
    Returns:
        hashable_tokens (tuple): Hashable, standardised tuple of tokens
    """
    s = set(string_to_split.split(" ")) # unique tokens
    if '' in s:
        s.remove('') # ignore empty tokens
    return tuple(sorted(s))  # standardise order, and make hashable


def process_name(name):
    """Remove punctuation, lowercase and then return hasbable tokens.

    Args:
        name (str): String to process
    Returns:
        hashable_tokens (tuple): Hashable, standardised tuple of tokens.
    """
    trans = str.maketrans(PUNCT, ' '*len(PUNCT)) # map punct to space
    _name = name.translate(trans)  # remove punctuation
    _name = _name.lower()  # lowercase
    return hashable_tokens(_name)


def append_disputed_countries(grid_ctrys):
    """Add "disputed aliases" to the list of GRID countries,
    to "forgive" either GRID or the matching dataset for
    using the "wrong" country, in the case of disputed countries.

    Args:
        grid_ctrys (set): Set of countries to be extended with disputed aliases.
    Returns:
        _grid_ctrys (set): Extended set of countries, with disputed aliases included.
    """
    disputed_ctrys = get_disputed_countries()  # Note: lru_cached
    _disputed_ctrys = set()
    for ctry in grid_ctrys:
        if ctry not in disputed_ctrys:
            continue
        _disputed_ctrys.add(disputed_ctrys[ctry])
    return grid_ctrys.union(_disputed_ctrys)


def _evaluate_matches(match_scores, ctry_code,
                      name_id_lookup, grid_ctry_lookup,
                      score_threshold=1,
                      multinat_threshold=3,
                      multimatch_threshold=2,
                      long_name_threshold=4)
    """Evaluate whether the list of proposed matches should be accepted,
    based on a series of criteria.

    Args:
        match_scores (dict): Precalculated match terms and scores
        ctry_code (str): Country code of the organisation we're
                         trying to match.
        name_id_lookup (dict): Lookup of GRID names to IDs
                               (including aliases)
        grid_ctry_lookup (dict): Lookup of GRID ID to country code
        score_threshold (float): Only consider matches with at
                                 least this score.
        multinat_threshold (int): Consider an organisation to be
                                  multinational if it is associated with
                                  this many countries. Multinational
                                  organisations with a matching names,
                                  but non-matching countries will be
                                  interpretted as good matches.
        multimatch_threshold (int): If this many aliases are matched,
                                    then accept the match, regardless
                                    of the country match.
        long_name_threshold (int): If the names have at least this many
                                   terms, they are considered "long names"
                                   Long names with exact matches will be
                                   accepted, regardless of the country
                                   match.
    """
    # Generate other form (if any) of this country's id
    disputed_ctrys = get_disputed_countries()  # Note: lru_cached
    other_ctry = (disputed_ctrys[ctry_code]
                  if ctry_code in disputed_ctrys else ctry_code)

    # Find a match
    found_gids = set()
    scores = Counter(match_scores).most_common()  # best scores first
    for name, score in scores:
        if score < score_threshold:
            continue
        # Generate set of GRID countries for these ids
        gids = name_id_lookup[name]
        grid_ctrys = set(grid_ctry_lookup[gid] for gid in gids)
        grid_ctrys = append_disputed_countries(grid_ctrys)

        # Country-based matching criteria
        # -------------------------------
        # weak criteria: name match, but no country code available
        no_ctry_code = ((ctry_code is None or grid_ctrys == {None})
                        and score == 1)
        # good criteria: name match, country match
        ctry_match = (ctry_code in grid_ctrys
                      or other_ctry in grid_ctrys)

        # Circumstantial matching criteria
        # (don't need country match if these are satisfied)
        # --------------------------------
        is_very_multinational =  (len(grid_ctrys) >= multinat_threshold)
        # matches found to multiple aliases
        multiple_matches = len(matches) >= multimatch_threshold
        # chance of accidentally matching a very long name seem
        # slim, unless matched fuzzily (score < 1)
        is_long_name = any(len(name) >= long_name_threshold
                           for name in matches) and score == 1

        # If none of the criteria, skip
        if not any((no_ctry_code, ctry_match,
                    is_very_multinational, multiple_matches,
                    is_long_name)):
            continue
        # Only take future scores if they're at least this good
        score_threshold = score
        found_gids = found_gids.union(gids)
    # Default case
    if len(found_gids) == 0:
        score_threshold = None
    return found_gids, score_threshold


class MatchEvaluator:
    """Evaluate whether the list of proposed matches should be accepted,
    based on a series of criteria.

    Args:
        match_scores (dict): Precalculated match terms and scores
        ctry_code (str): Country code of the organisation we're
                         trying to match.
        name_id_lookup (dict): Lookup of GRID names to IDs
                               (including aliases)
        grid_ctry_lookup (dict): Lookup of GRID ID to country code
        score_threshold (float): Only consider matches with at
                                 least this score.
        multinat_threshold (int): Consider an organisation to be
                                  multinational if it is associated with
                                  this many countries. Multinational
                                  organisations with a matching names,
                                  but non-matching countries will be
                                  interpretted as good matches.
        multimatch_threshold (int): If this many aliases are matched,
                                    then accept the match, regardless
                                    of the country match.
        long_name_threshold (int): If the names have at least this many
                                   terms, they are considered "long names"
                                   Long names with exact matches will be
                                   accepted, regardless of the country
                                   match.
    """
    def __init__(self, name_id_lookup, grid_ctry_lookup,
                 score_threshold=1, multinat_threshold=3,
                 multimatch_threshold=2, long_name_threshold=4):
        self.s_threshold = score_threshold
        self.mn_threshold = multinat_threshold
        self.mm_threshold = multimatch_threshold
        self.ln_threshold = long_name_threshold
        self.name_id_lookup = name_id_lookup
        self.grid_ctry_lookup = grid_ctry_lookup

    def evaluate_matches(self, ctry_code, match_scores):
        """Shallow wrapper around :obj:`_evaluate_matches`,
        where most arguments have already been passed to `__init__`."""
        return _evaluate_matches(match_scores=match_scores,
                                 ctry_code=ctry_code,
                                 name_id_lookup=self.name_id_lookup,
                                 grid_ctry_lookup=self.grid_ctry_lookup,
                                 score_threshold=self.s_threshold,
                                 multinat_threshold=self.mn_threshold,
                                 multimatch_threshold=self.mm_threshold,
                                 long_name_threshold=self.ln_threshold)


    def generate_matches(self, data):
        matches, remaining_names = find_exact_matches(data)
        jaccard_matches = fast_nested_jaccard(remaining_names, 
                                              self.all_grid_names)
        inexact_matches = find_inexact_matches(data, matches, 
                                               jaccard_matches)
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
