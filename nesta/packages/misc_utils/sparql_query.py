"""
sparql_query
------------

A wrapper to SPARQLWrapper to query a given SPARQL endpoint
with rate-limiting.

"""
from SPARQLWrapper import SPARQLWrapper, JSON


def sparql_query(endpoint, query, nbatch=5000, test=False):
    """Query a given endpoint with rate-limiting in 'nbatch' batches

    Args:
        endpoint (str): SPARQL endpoint URL.
        query (str): SPARQL query string.
        nbatch (int): Batch size.
    Returns:
        (:obj:`list` of :obj:`dict`): Batch of results as a list of dictionaries
    """
    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat(JSON)

    # Execute the query in batches of nbatch
    n = 0
    while True:
        # Run the query and get the results
        batch_query = f"{query} LIMIT {nbatch} OFFSET {n}"
        sparql.setQuery(batch_query)
        results = sparql.query().convert()

        # Extract the data values from the results
        data = results["results"]["bindings"]
        n_results = len(data)
        if n_results == 0:
            break
        else:
            # Extract values for each item
            clean_batch = [{k: v['value'] for k, v in row.items()} for row in data]
            n += nbatch
            yield clean_batch

        if test:
            break
