'''
sparql_query
------------

A wrapper to SPARQLWrapper to query a given SPARQL endpoint
with rate-limiting.

'''
from SPARQLWrapper import SPARQLWrapper, JSON

def sparql_query(endpoint, query, nbatch=5000, test=False):
    '''Query a given endpoint with rate-limiting in
    'nbatch' batches

    Args:
        endpoint (str): SPARQL endpoint URL.
        query (str): SPARQL query string.
        nbatch (int): Batch size.
    Returns:
        data (list): List of dictionary of results
    '''
    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat(JSON)
    cols,data_values = [],[]
    # Execute the query in batches of nbatch
    n, n_results = 0, 1  # Init with any value greater than 0
    while n_results > 0:
        # Run the query and get the results
        sparql.setQuery(query+" LIMIT "+str(nbatch)+" OFFSET "+str(n))
        results = sparql.query().convert()   
        # Extract the data values from the results
        data = results["results"]["bindings"]
        n_results = len(data)
        if n_results > 0:
            cols = [k for k,v in data[0].items()] # Get column names
            for line in data:
                values = [v["value"] for k,v in line.items()] 
                data_values.append(values)             
        n += nbatch
        if test:
            break
    # Return column names and column values
    data = []
    for row in data_values:
        new_row = {col: value for col, value in zip(cols, row)}
        data.append(new_row)
    return data
