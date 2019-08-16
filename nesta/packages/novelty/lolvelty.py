import numpy as np

def lolvelty(es, index, doc_id, fields,
             similar_perc=25, 
             max_query_terms=25,
             max_doc_frac=0.75, 
             minimum_should_match=0.3,
             human_friendly=True,
             total=None):
    """Simple statistical measurement of novelty. First,
    the 1000 most similar documents are retrieved 
    from elasticsearch, scored by tfidf and normalised to
    the document itself. Novelty is then defined as
    (1 - quantile), where the quantile is defined by the user.
    "Novel" documents are intuitively very seperated from lower
    quantile (i.e. unrelated) documents, and so using a quantile
    of e.g. 25% leads to intuitive results. 

    The score is optionally, rescaled to have a human-friendly 
    scale, since humans don't deal well with fractions.

    Args:
        es (elasticsearch.Elasticsearch): Elasticsearch object.
        index (str): Elasticsearch index to query.
        doc_id (str): Document id in Elasticsearch to rank.
        fields (list): List of fields to determine novelty from.
        max_query_terms (int): Maximum number of terms to determine 
                               similarity from.
        max_doc_frac (float): Maximum fraction of documents a term can
                              be present in (cuts out stop words).
        minimum_should_match (float): Minimum number of query terms that 
                                      should be present in all documents.
        total (int): Total count of documents in the index. Pass this in 
                     to save a little processing time.
    Returns:
        score (float): A novelty score.
    """
    # Calculate total if required
    if total is None:
        r = es.count(index=index,
                     body={"query": {"match_all": {}}})
        total = r['count']
    # Build mlt query
    max_doc_freq = int(max_doc_frac*total)
    minimum_should_match=str(int(minimum_should_match*100))
    mlt_query = {
        "query": {
            "more_like_this": {
                "fields": fields, 
                "like": [{'_id':doc_id,  
                          '_index':index}], 
                "min_term_freq": 1, 
                "max_query_terms": max_query_terms, 
                "min_doc_freq": 1,
                "max_doc_freq": max_doc_freq, 
                "boost_terms": 1., 
                "minimum_should_match": minimum_should_match,
                "include": True
            }
        },
        "size":1000,
        "_source":["_score"]
    }
    # Make the search and normalise the scores
    r = es.search(index=index, body=mlt_query) 
    scores = [h['_score']/r['hits']['max_score'] 
              for h in r['hits']['hits']]
    if len(scores) <= 1:
        return None
    # Calculate the novelty
    delta = np.percentile(scores, similar_perc)
    if human_friendly:
        return 10*(similar_perc - 100*delta)
    return 1 - delta
