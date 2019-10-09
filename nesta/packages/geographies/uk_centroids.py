from nesta.packages.misc_utils.sparql_query import sparql_query
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub
import requests

ENDPOINT = "http://statistics.data.gov.uk/sparql"
LSOA11_TTWA11_LU = 'https://opendata.arcgis.com/datasets/50ce6db9e3a24f16b3f63f07e6a069f0_0.geojson'

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
    I am assuming that this method will return all possible TTWAs,
    both from 2001 and 2011 - it might not be the best, but might be
    useful.

    Return:
            A list of TTWA IDs.
    """

    CONFIG = find_filepath_from_pathstub("fetch_ttwa_codes.sparql")
    n = 1 if test else None
    with open(CONFIG) as f:
        query = f.read().replace("\n", " ")
    data = sparql_query(ENDPOINT, query, batch_limit=n)
    print([row for batch in data for row in batch])
    return [row["area_code"] for batch in data for row in batch]

def get_ttwa11_codes():
    '''
    Collect TTWA11 codes from fixed URI on ONS database.
    It is specifically linked to TTWA from 2011 Census.

    Return:
            A list of TTWA IDs.
    '''
    return True

def ttwa_to_lsoa(test=False):
    '''
    Returns a list of string pairs (TTWA ID, LSOA IDs) (both str)

    Example:
    get_ttwa_to_lsoa('E30000261') --> [LSOAs]
    '''
    r_json = requests.get(LSOA11_TTWA11_LU).json()
    assert('features' in r_json)
    children = r_json['features']
    assert('properties' in children[0])
    if test:
        children = children[:10]
    table_lu = [(child['properties']['TTWA11CD'], child['properties']['LSOA11CD'])
                            for child in children]
    return table_lu

def lsoa_to_oa():
    return False
