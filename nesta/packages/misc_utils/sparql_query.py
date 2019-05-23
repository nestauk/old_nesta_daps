"""
sparql_query
------------

A wrapper to SPARQLWrapper to query a given SPARQL endpoint
with rate-limiting.

"""
import logging
from SPARQLWrapper import SPARQLWrapper, JSON
from SPARQLWrapper.SPARQLExceptions import EndPointNotFound
import time


def sparql_query(endpoint, query, nbatch=5000, batch_limit=None):
    """Query a given endpoint with rate-limiting in 'nbatch' batches

    Args:
        endpoint (str): SPARQL endpoint URL.
        query (str): SPARQL query string.
        nbatch (int): Batch size.
        batch_limit (int): limit the number of batches, for testing
    Returns:
        (:obj:`list` of :obj:`dict`): Batch of results as a list of dictionaries
    """
    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat(JSON)

    # Execute the query in batches of nbatch
    n = 0
    total_batches = 0
    retry_attempts = 5
    retry_delay = 5  # seconds
    while True:
        # Run the query and get the results
        batch_query = f"{query} LIMIT {nbatch} OFFSET {n}"
        sparql.setQuery(batch_query)
        retries = retry_attempts
        results = None
        while retries > 0 and not results:
            try:
                results = sparql.query().convert()
            except EndPointNotFound:
                logging.warning("Couldn't connect to endpoint")
                time.sleep(retry_delay)
                retries -= 1

        # Extract the data values from the results
        data = results["results"]["bindings"]
        n_results = len(data)
        if n_results == 0:
            break
        else:
            # Extract values for each item
            clean_batch = [{k: v['value'] for k, v in row.items()} for row in data]
            n += n_results
            yield clean_batch

        total_batches += 1

        if n_results < nbatch:
            # less than full batch returned
            break
        if batch_limit and total_batches >= batch_limit:
            break
