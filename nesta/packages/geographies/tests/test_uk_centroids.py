#import sys
#sys.path.insert(1,'/Users/stefgarasto/Google Drive/Documents/scripts/193_ttwa_travel/nesta')
from unittest import mock
import pytest
from nesta.packages.geographies.uk_centroids import get_ttwa_codes
from nesta.packages.geographies.uk_centroids import ttwa11_to_lsoa11
from nesta.packages.geographies.uk_centroids import lsoa_to_oa
from nesta.packages.geographies.uk_centroids import get_ttwa_name_from_id
from nesta.packages.geographies.uk_centroids import get_ttwa_id_from_name
from nesta.packages.geographies.uk_centroids import get_ttwa11_codes
from nesta.packages.geographies.uk_centroids import hit_odarcgis_api
import unittest
import time

SPARQL_QUERY = '''
PREFIX entity: <http://statistics.data.gov.uk/def/statistical-entity#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?area_code
WHERE {
    ?area_entity entity:code ?area_code_entity;
    rdfs:label ?area_code .
    ?area_code_entity rdfs:label ?area_code_type;
    FILTER(SUBSTR(?area_code_type, 1, 3) IN("S22","K01","N12","W22","E30")).
}
'''

QUERY_RESPONSE = [[{'area_code': 'E30000195'}, {'area_code': 'E30000196'},
{'area_code': 'E30000197'}, {'area_code': 'E30000193'},
{'area_code': 'E30000194'}, {'area_code': 'E30000200'},
{'area_code': 'E30000199'}, {'area_code': 'E30000201'},
{'area_code': 'E30000198'}, {'area_code': 'E30000187'}]]

TTWA11_LIST = 'https://opendata.arcgis.com/datasets/9ac894d3086641bebcbfa9960895db39_0.geojson'

def test_hit_odarcgis_api():
    out = hit_odarcgis_api(TTWA11_LIST,test=True)
    assert 'properties' in out[0]
    assert len(out)==10


@mock.patch("nesta.packages.geographies.uk_centroids.find_filepath_from_pathstub",
            return_value=None)
@mock.patch("builtins.open", new_callable=mock.mock_open, read_data=SPARQL_QUERY)
@mock.patch("nesta.packages.geographies.uk_centroids.sparql_query",
            return_value=QUERY_RESPONSE)
def test_get_ttwa_codes(mocked_open, mocked_find_filepath_from_pathstub,
                                                        mocked_sparql_query):
    codes = get_ttwa_codes(test=True)
    assert isinstance(codes, list)
    #this is because I hardcoded the QUERY_RESPONSE above and I want to make sure
    #it's getting all and only all of the relevant information
    assert len(codes)==10
    assert 'E30000198' in codes

def test_get_ttwa11_codes():
    # check there is not a more recent Census (2011 TTWAs are valid until 2021
    # included
    current_year = time.strftime("%Y")
    assert int(current_year)<=2021

    out = get_ttwa11_codes()
    assert isinstance(out, list)
    assert all(len(ttwa)==9 for ttwa in out)
    # uniqueness
    assert len(set(out)) == len(out)

def test_ttwa11_to_lsoa11():
    # check there is not a more recent Census (2011 TTWAs are valid until 2021
    # included
    current_year = time.strftime("%Y")
    assert int(current_year)<=2021

    out = ttwa11_to_lsoa11(test=True)
    # all should be strings
    assert all(isinstance(ttwa,str) % isinstance(lsoa,str) for ttwa,lsoa in out)
    # all TTWA and LSOA codes should be of length 9
    assert all(len(ttwa)==9 & len(lsoa)==9 for ttwa,lsoa in out)
    # there should only be unique elements
    assert len(set(out)) == len(out)
    #this is because in test mode I should only have 10 elements
    assert len(out)==10
    # assert I'm only picking up good ttwas
    ttwa11_list = get_ttwa11_codes()
    assert all(ttwa in ttwa11_list for ttwa,_ in out)

#def test_get_lsoa_to_oa():
#    # placeholder
#    assert 2==2

#if __name__ == "__main__":
#    unittest.main()
