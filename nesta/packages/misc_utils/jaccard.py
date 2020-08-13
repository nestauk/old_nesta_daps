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
    else
        return Counter(results).most_common(1)[0]


def _nested_jaccard(unmatched_A, unmatched_B, nested_threshold):
    intersection = []
    for item_a in unmatched_A:
        item_b, _score = best_jaccard(item_a, unmatched_B)
        if _score is not None and _score >= nested_threshold:
            score = _score
            unmatched_B.remove(item_b)
            to_remove.add(item_a)
        else:
            score = 0
        intersection.append(score)

    for item_a in to_remove:
        unmatched_A.remove(item_a)    
    return intersection


def nested_jaccard(a, b, nested_threshold=0):
    A, B, intersection, union =_prepare_jaccard(a, b)
    intersection = [1]*len(intersection)
    unmatched_A = A - B
    unmatched_B = B - A
    intersection += _nested_jaccard(unmatched_A, unmatched_B, nested_threshold)
    intersection += _nested_jaccard(unmatched_B, unmatched_A, nested_threshold)
    return jaccard(intersection, union)
