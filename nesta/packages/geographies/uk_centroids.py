#from nesta.packages.geographies.uk_geography_lookup import get_gss_codes
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub


def get_ttwa_name_from_id(ttwa_id):
    """
    Returns the NAME (string) of the TTWA given its ID (string).
    Example: get_ttwa_name_from_id('E30000261') = 'Sheffield'
    """
    return False

def get_ttwa_id_from_name(ttwa_name):
    """
    Returns the ID (string) of the TTWA given its NAME (string).
    Example: get_ttwa_name_from_id('Sheffield') = 'E30000261'
    """
    assert()
    return False

def get_ttwa_codes(test=False):
    """
    Collects all TTWA IDs from the ONS database.

    Return:
            A list of TTWA IDs.
    """

    CONFIG = find_filepath_from_pathstub("fetch_ttwa_codes.sparql")
    n = 1 if test else None
    with open(CONFIG) as f:
        query = f.read().replace("\n", " ")
    data = sparql_query(ENDPOINT, query, batch_limit=n)
    return [row["area_code"] for batch in data for row in batch]

def ttwa_to_lsoa():
    '''
    Returns a list of LSOAs IDs (str) in a given TTWA based on the TTWA ID (str).
    Example:
    get_ttwa_to_lsoa('E30000261') --> []
    '''
    return False

def lsoa_to_oa():
    return False
