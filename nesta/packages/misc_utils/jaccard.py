"""
jaccard
=======

Tools for measuring Jaccard similarity, specifically designed
for fast fuzzy matching via SetSimilaritySearch.

This uses a "fuzzy" intersection in a redefinition of the Jaccard similarity. To understand, consider the two sets:

    a = {"open", "university"}
    b = {"apen", "university"}

The regular Jaccard similarity between these is 1/3. Instead, if we define the similarity score as the sum of the closest jaccard matches between the terms:

   jaccard("open", "apen") ==> 3/5
   jaccard("university", "university") ==> 1

Then the total Jaccard similarity can be reinterpretted as (3/5 + 1)/2 = 8/10. Using this via SetSimilaritySearch offers significant speed-ups with respect to fuzzywuzzy.
"""

from collections import defaultdict
from collections import Counter
from SetSimilaritySearch import SearchIndex


def _prepare_jaccard(a, b):
    """Convert inputs to sets and perform intersection and union
    operations, as inputs for Jaccard similarity calculation.

    Args:
        {a, b} (iterable): Objects to calculate Jaccard index between.
    Returns:
        {A, B, intersection, union}: python sets of the input objects, as well as the intersection and union objects.
    """
    A = set(a)
    B = set(b)
    intersection = A.intersection(B)
    union = A.union(B)
    return A, B, intersection, union


def _jaccard(intersection, union):
    """Size of the intersection divided by the size of the union.

    Args:
        {intersection, union}: The intersection and union of two sets, respectively
    Returns:
        score (float): The Jaccard similarity index for these sets.
    """
    return len(intersection) / len(union)


def jaccard(a, b):
    """Calculate the jaccard similarity between sets a and b.

    Args:
        {a, b} (iterable): Objects to calculate Jaccard index between
    Returns:
        score (float): The Jaccard similarity index for these sets.
    """
    _, _, intersection, union = _prepare_jaccard(a, b)
    return _jaccard(intersection, union)


def best_jaccard(a, bs):
    """Compare to a list of sets, and return the best result.

    Args:
        a (iterable): Object to query for closest match.
        bs (iterable of iterables): Objects to query for closest match.
    Returns:
        {best, score}: The closest object, and it's matching score.
    """
    results = {b: jaccard(a, b) for b in bs}
    if len(results) == 0:
        return None, None  # 2-tuple to be consistent with Counter.most_common
    else:
        return Counter(results).most_common(1)[0]


def _nested_intersection_score(unmatched_A, unmatched_B, nested_threshold):
    """Calculate sum of best Jaccard similarity scores between two sets.
    Args:
        unmatched_{A, B} (set): Iterable to compare items between.
        nested_threshold (float): Ignore similarity scores worse than this threshold (helps to avoid assigning scores between e.g. long words)
    Returns:
        intersection (list): List of best Jaccard similarity scores between the two sets, to be used as a proxy intersection in calculating the total Jaccard similarity.
    """
    intersection = []
    to_remove = set()
    for item_a in unmatched_A:
        item_b, _score = best_jaccard(item_a, unmatched_B)
        if _score is None or _score <= nested_threshold:
            continue
        unmatched_B.remove(item_b)
        to_remove.add(item_a)
        intersection.append(_score)

    for item_a in to_remove:
        unmatched_A.remove(item_a)
    return intersection


def nested_jaccard(a, b, nested_threshold=0.7):
    """Calculate the nested ("fuzzy") Jacard score between two objects, which have elements which themselves are hashable and iterable.

    Args:
        {a, b}: (iterable): Objects to calculate Jaccard index between.
        nested_threshold (float): Ignore similarity scores worse than this threshold (helps to avoid assigning scores between e.g. long words)
    Returns:
        score (float): Nested jaccard similarity between input objects.
    """
    A, B, intersection, _ = _prepare_jaccard(a, b)
    insct_scores = [1]*len(intersection)
    unmatched_A = A - B
    unmatched_B = B - A
    insct_scores += _nested_intersection_score(unmatched_A,
                                               unmatched_B,
                                               nested_threshold)
    insct_scores += _nested_intersection_score(unmatched_B,
                                               unmatched_A,
                                               nested_threshold)
    union_len = len(insct_scores) + len(unmatched_A) + len(unmatched_B)
    return sum(insct_scores)/union_len


def fast_jaccard(terms_to_index, terms_to_query, kernel=jaccard,
                 similarity_threshold=0.5, single_term_threshold=0.75,
                 **kwargs):
    """Fast Jaccard index lookup via SetSimilaritySearch, and then applying a choice of kernels to recalculate the Jaccard index, if required.
    
    Args:
        terms_to_index (iterable): Terms to be indexed by SetSimilaritySearch.
        terms_to_query (iterbale): Terms to be queried against the indexed terms for Jaccard similarity.
        kernel (function): Function which calculates the similarity between two terms.
        similarity_threshold (float): Do not re-evaluate similiarities between terms with a strict Jaccard similarity less than this value. Threshold=0.5 in our context should be interpreted as meaning that if there are two terms, require one to match exactly.
    Returns:
        jaccard_matches (dict of dict of float): Jaccard similarity scores between near matching
    """
    terms_to_index = list(terms_to_index)
    single_terms = [terms[0] if len(terms) == 1 else (None, )
                    for terms in terms_to_index]
    search_single_index = SearchIndex(single_terms, similarity_threshold=single_term_threshold,
                                      similarity_func_name="jaccard")
    search_index = SearchIndex(terms_to_index, similarity_threshold=similarity_threshold,
                               similarity_func_name="jaccard")
    jaccard_matches = defaultdict(dict)
    for query_term in terms_to_query:
        # Make initial search over sets
        for idx, DELETEME in search_index.query(query_term):
            index_term = terms_to_index[idx]
            jaccard_matches[index_term][query_term] = kernel(index_term, query_term, **kwargs)
        if len(query_term) != 1:
            continue
        # If there are single terms, check those too
        for idx, DELETEME in search_single_index.query(query_term[0]):
            index_term = single_terms[idx]
            if index_term == (None,):
                continue
            index_term = terms_to_index[idx]
            if query_term not in jaccard_matches[index_term]:
                jaccard_matches[index_term][query_term] = kernel(index_term, query_term, **kwargs)
    return jaccard_matches


def fast_nested_jaccard(terms_to_index, terms_to_query, 
                        similarity_threshold=0.5,
                        nested_threshold=0.8,
                        single_term_threshold=0.75):
    """Superficial wrapper of :obj:`fast_jaccard`, with :obj:`kernel` set to :obj:`nested_jaccard`"""    
    return fast_jaccard(terms_to_index, terms_to_query, 
                        kernel=nested_jaccard, 
                        similarity_threshold=similarity_threshold,
                        nested_threshold=nested_threshold,
                        single_term_threshold=single_term_threshold)
