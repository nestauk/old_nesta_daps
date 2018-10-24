from collections import defaultdict


def dedup(docs, depth_cut=lambda depth: pow(5, depth),
          depth=0):
    '''Efficient recursive deduplication of large very similar iterables.

    Args:
        docs (dict): A dictionary mapping a unique identifier key to
                     an iterable (e.g. [but not restricted to]
                     a continuous text body, :code:`list` of `str`)
        depth_cut (:obj:`method`): A method to calculate the upper index
                     at which to compare values in :code:`docs`, as a function
                     of :code:`docs`. The default method uses powers of 5; i.e.
                     documents will be compared recursively at the 1st
                     character and then the 5th, 25th etc until the documents
                     are either classified as matching or not.
    Returns:
        deduped (dict): A mapping between each document's unique id and
                        the first document id to which it is identical. The
                        unduplicated documents can be infered for those with
                        an identical key and value.
    '''
    # First step is to split documents by length
    deduped = {}
    if depth == 0:
        for _docs in split_by_length(docs):
            # Now iteratively dedup
            _deduped = dedup(_docs, depth_cut=depth_cut,
                             depth=depth+1)
            # ... and append these
            for k, v in _deduped.items():
                deduped[k] = v
        return deduped

    # The assumption is that if you have got this far, we
    # are beyond depth == 0

    # Generate mapping between document stubs and ids
    inverse_docs = defaultdict(list)
    maxed_ids = set()
    n = depth_cut(depth)
    for id_, doc in docs.items():
        # Check whether the document length has been reached
        n_cut = n
        if n > len(doc):
            n_cut = len(doc)
            maxed_ids.add(id_)
        # Map the stub to its id
        inverse_docs[doc[0: n_cut]].append(id_)

    # Deduplicate
    for _, ids in inverse_docs.items():
        # Unique docs are (by definition) deduped
        if len(ids) == 1:
            deduped[ids[0]] = ids[0]
            continue
        # If max doc length has been reached then check ids are dupes
        dupes = [id_ for id_ in ids if id_ in maxed_ids]
        for id_ in dupes:
            deduped[id_] = dupes[0]
        # Otherwise recursively find dupes...
        remaining = {id_: docs[id_] for id_ in ids
                     if id_ not in maxed_ids}
        _deduped = dedup(remaining, depth_cut=depth_cut,
                         depth=depth+1)
        # ... and append these
        for k, v in _deduped.items():
            deduped[k] = v
    # Done
    return deduped


def split_by_length(docs):
    '''Split documents into groups by their length, and yield each group
    Args:
        docs (dict): A dictionary mapping a unique identifier key to
                     an iterable (e.g. [but not restricted to]
                     a continuous text body, :code:`list` of `str`)
    Yields:
        :obj:`dict` chunks of the input documents, each of the same length.
    '''
    docs_by_length = defaultdict(dict)
    for id_, doc in docs.items():
        docs_by_length[len(doc)][id_] = doc
    for _, _docs in docs_by_length.items():
        yield _docs
