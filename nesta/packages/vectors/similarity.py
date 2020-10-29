from nesta.packages.vectors.read import download_vectors
import faiss


def find_similar_vectors(data, ids, k=10, k_large=1000, 
                         metric=faiss.METRIC_L2, score_threshold=0.5):
    n, d = data.shape
    k = n if k > n else k    
    k_large = n if k_large > n else k_large
    index = faiss.IndexFlat(d, metric)
    index.add(data)
    
    # Make an expansive search to determine the base level of similarity in this space
    # as the mean similarity of documents in the close vicinity
    D, I = index.search(data, k_large) 
    base_similarity = D.mean(axis=1)
    
    # Now subset only the top k results
    D = D[:,:k]
    I = I[:,:k]
    
    similar_vectors = {}
    for _id, row, sims, base in zip(ids, ids[I], D, base_similarity):
        _id = str(_id)
        scores = (base - sims) / base
        over_threshold = scores > score_threshold
        if over_threshold.sum() <= 1:
            continue
        results = {i:s for i, s in zip(row, scores) 
                   if s > score_threshold and _id != i and i not in similar_vectors}
        if len(results) == 0:
            continue
        similar_vectors[_id] = results        
    return similar_vectors


def generate_duplicate_links(orm, id_field, database, k=10, k_large=1000, 
                             metric=faiss.METRIC_L2, duplicate_threshold=0.9,
                             read_chunksize=10000, read_max_chunks=None):
    """Convenience method for selecting knn for each vector in database,
    by reading all vectors into memory, indexing with FAISS and then
    selecting only very similar vectors as being "duplicates".
    
    Similarity is determined by the given metric parameter. For high-dim
    vectors, such as those generated by BERT transformers 
    this should be faiss.METRIC_L1. Explicitly, documents with

        (mean(D_large) - D) / mean(D_large) > duplicate_threshold

    are counted as "duplicates" of each other, where D_large is a vector of
    distances of the k_large nearest neighbours, and D is a vector of 
    distances of the k nearest neighbours.

    Args:
        orm ():
        id_field ():
        database ():
        k (): 10
        k_large (): 1000
        metric (): faiss.METRIC_L2
        duplicate_threshold (): 0.9
        read_chunksize (): 10000
        read_max_chunks (): None

    Returns:
        links (json): Rows containing the ids of matching documents 
                      and their match weight.
    """
    # Read the data
    data, ids = download_vectors(orm=orm, id_field=id_field, database=database,
                                 chunksize=read_chunksize, max_chunks=read_max_chunks)
    # Find all sets of similar vectors
    similar_vectors = find_similar_vectors(data=data, ids=ids, k=k,
                                           k_large=k_large, metric=metric,
                                           score_threshold=duplicate_threshold)
    # Clean up
    del data
    del ids
    # Structure the output for ingestion to the database as a link table
    links = [{f"{id_field}_1": _id1, f"{id_field}_2": _id2, "weight": weight} 
             for _id1, sims in similar_vectors.items() 
             for _id2, weight in sims.items()]
    return links