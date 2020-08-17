from collections import defaultdict
from collections import Counter
from SetSimilaritySearch import SearchIndex

def _prepare_jaccard(a, b):
    A = set(a)
    B = set(b)
    intersection = A.intersection(B)
    union = A.union(B)
    return A, B, intersection, union


def _jaccard(intersection, union):
    return len(intersection) / len(union)


def jaccard(a, b):
    _, _, intersection, union = _prepare_jaccard(a, b)
    return _jaccard(intersection, union)


def best_jaccard(a, bs):
    results = {b: jaccard(a, b) for b in bs}
    if len(results) == 0:
        return None, None  # 2-tuple to be consistent with Counter.most_common
    else:
        return Counter(results).most_common(1)[0]


def _nested_intersection_score(unmatched_A, unmatched_B, nested_threshold):
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


def nested_jaccard(a, b, nested_threshold=0):
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


def fast_jaccard(terms_to_index, terms_to_query, kernel=jaccard):
    terms_to_index = list(terms_to_index)
    search_index = SearchIndex(terms_to_index, similarity_threshold=0.5)
    jaccard_matches = defaultdict(dict)
    for query_term in terms_to_query:
        for idx, _ in search_index.query(query_term):
            index_term = terms_to_index[idx]
            jaccard_matches[index_term][query_term] = kernel(index_term, query_term)
    return jaccard_matches


def fast_nested_jaccard(terms_to_index, terms_to_query):
    return fast_jaccard(terms_to_index, terms_to_query, kernel=nested_jaccard)
