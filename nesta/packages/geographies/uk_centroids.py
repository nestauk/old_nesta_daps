from nesta.packages.misc_utils.sparql_query import sparql_query
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub
import requests
from shapely.geometry import Polygon
from shapely import wkt

ENDPOINT = "http://statistics.data.gov.uk/sparql"
LSOA11_TTWA11_LU = 'https://opendata.arcgis.com/datasets/50ce6db9e3a24f16b3f63f07e6a069f0_0.geojson'
TTWA11_LIST = 'https://opendata.arcgis.com/datasets/9ac894d3086641bebcbfa9960895db39_0.geojson'
LSOA11_LIST = 'https://opendata.arcgis.com/datasets/3ce71e53d9254a73b3e887a506b82f63_0.geojson'
#ARCGIS_BASE = ''.join(['https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/',
#    'services/{}/FeatureServer/0/query'])
OA11_LSOA11_MSOA11_LU = 'https://opendata.arcgis.com/datasets/6ecda95a83304543bc8feedbd1a58303_0.geojson'
OA11_REGION_LU = 'https://opendata.arcgis.com/datasets/1c2f7b13918d4e7286448bdc6458b415_0.geojson'
#NSPL = 'https://opendata.arcgis.com/datasets/055c2d8135ca4297a85d624bb68aefdb_0.geojson'
# NSPL query url: https://ons-inspire.esriuk.com/arcgis/rest/services/Postcodes/NSPL_Latest/MapServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json
# https://ons-inspire.esriuk.com/arcgis/rest/services/Postcodes/NSPL_Latest/MapServer/0/query?where=1%3D1&outFields=objectid,pcd,pcd2,oa11,msoa11,lsoa11,ttwa&outSR=4326&f=json
# one query will only return 33000 entries

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
    return False

def hit_odarcgis_api(api_code, test=False):
    '''
    Get the response from the given API URL (opendata.arcgis.com)

    Returns the list of dicts with the relevant features
    '''
    r = requests.get(api_code)
    assert r.status_code == 200
    r = r.json()
    assert('features' in r)
    children = r['features']
    if test & (len(children)>9):
        children = children[:10]
    return children


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
    #out = [row["area_code"] for batch in data for row in batch]
    #print(out)
    return [row["area_code"] for batch in data for row in batch]


def get_ttwa11_codes(test=False):
    #TODO: a dict ID : NAME would be better but then it's incompatible with
    #get_ttwa_codes
    '''
    Collect TTWA11 codes for UK from fixed URI on ONS database.
    It is specifically linked to TTWA from 2011 Census.

    Return:
            A list of TTWA IDs.
    '''
    children = hit_odarcgis_api(TTWA11_LIST, test=test)
    return [child['properties']['TTWA11CD'] for child in children]

def get_lsoa11_codes(test=False):
    #TODO: a dict ID : NAME would be better but then it's incompatible with
    #get_ttwa_codes
    '''
    Collect LSOA11 codes for ENGLAND and WALES from fixed URI on ONS database.
    It is specifically linked to LSOAs from 2011 Census.

    Return:
            A list of LSOA IDs.
    '''
    children = hit_odarcgis_api(LSOA11_LIST, test=test)
    return [child['properties']['LSOA11CD'] for child in children]

def get_oa11_codes(test=False):
    '''
    Collect OA11 codes for ENGLAND and WALES and their respective region
    from fixed URI on ONS database.
    It is specifically linked to OAs from 2011 Census.

    Return:
            A list of OA IDs and an empty list.
            [in future can be augmented with lookup to regions]
    '''
    children = hit_odarcgis_api(OA11_REGION_LU, test=test)
    return [child['properties']['OA11CD'] for child in children], []

def ttwa11_to_lsoa11(test=False):
    '''
    Collects the lookup table between TTWAs and LSOAs (both from 2011 Census)
    from the ONS website and parses it to return all possible pairs
    (for the whole of UK)

    Returns a list of string pairs (TTWA ID, LSOA IDs) (both str)

    '''
    children = hit_odarcgis_api(LSOA11_TTWA11_LU,test=test)
    table_lu = [(child['properties']['TTWA11CD'], child['properties']['LSOA11CD'])
                            for child in children]
    return table_lu

def lsoa11_to_oa11(test=False):
    '''
    Collects the lookup table between LSOAs and OAs (both from 2011 Census)
    from the ONS website and parses it to return all possible pairs
    (for ENGLAND and WALES only)

    Returns two lists of string pairs:
     (OA ID, LSOA IDs) (both str)
     (OA ID, MSOA IDs) (both str)

    '''
    children = hit_odarcgis_api(OA11_LSOA11_MSOA11_LU,test=test)
    table_lu_lsoa = [(child['properties']['LSOA11CD'], child['properties']['OA11CD'])
                            for child in children]
    table_lu_msoa = [(child['properties']['MSOA11CD'], child['properties']['OA11CD'])
                            for child in children]
    return table_lu_lsoa, table_lu_msoa

def get_oa_centroids(n_start=0, n_end=1000):
    # TODO: batch this
    oa_codes, _ = get_oa11_codes()
    oa_centroids = []
    for oa_code in oa_codes[n_start:n_end]:
        r = requests.get(''.join(['http://statistics.data.gov.uk/resource.json?',
        'uri=http%3A%2F%2Fstatistics.data.gov.uk%2Fid%2Fstatistical-geography%2F',
        '{}%2Fgeometry'.format(oa_code)]))
        r = r.json()
        # there are many assumptions here
        assert('http://www.opengis.net/ont/geosparql#asWKT' in r[0])
        polygon_string = r[0]['http://www.opengis.net/ont/geosparql#asWKT']
        assert '@value' in polygon_string[0]
        polygon_string = polygon_string[0]['@value']
        # pass the WKT to shapely
        polygon = wkt.loads(polygon_string) #Polygon(polygon_list)
        coordinates = *polygon.centroid.coords # returned as LONG, LAT
        # store it as LAT, LONG
        oa_centroids.append((oa_code, coordinates[1], coordinates[0]))
    return oa_centroids
