#import sys
#sys.path.insert(1,'/Users/stefgarasto/Google Drive/Documents/scripts/193_ttwa_travel/nesta')
from unittest import mock
import pytest
import time
from nesta.packages.geographies.uk_centroids import get_ttwa_codes
from nesta.packages.geographies.uk_centroids import ttwa11_to_lsoa11
from nesta.packages.geographies.uk_centroids import lsoa11_to_oa11
from nesta.packages.geographies.uk_centroids import get_ttwa_name_from_id
from nesta.packages.geographies.uk_centroids import get_ttwa_id_from_name
from nesta.packages.geographies.uk_centroids import get_ttwa11_codes
from nesta.packages.geographies.uk_centroids import get_lsoa11_codes
from nesta.packages.geographies.uk_centroids import get_oa11_codes
from nesta.packages.geographies.uk_centroids import hit_odarcgis_api
from nesta.packages.geographies.uk_centroids import get_oa_centroids
from nesta.packages.geographies.uk_centroids import TTWA11_LIST,LSOA11_LIST

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

def test_hit_odarcgis_api():
    out = hit_odarcgis_api(TTWA11_LIST,test=True)
    assert 'properties' in out[0]
    assert len(out)==10


@mock.patch("nesta.packages.geographies.uk_centroids.find_filepath_from_pathstub",
            return_value=None)
@mock.patch("builtins.open", new_callable=mock.mock_open, read_data=SPARQL_QUERY)
#@mock.patch("nesta.packages.geographies.uk_centroids.sparql_query",
#            return_value=QUERY_RESPONSE)
def test_get_ttwa_codes(mocked_open, mocked_find_filepath_from_pathstub):#,
                                                        #mocked_sparql_query):
    codes = get_ttwa_codes(test=False)
    assert isinstance(codes, list)
    #this is because I hardcoded the QUERY_RESPONSE above and I want to make sure
    #it's getting all and only all of the relevant information
    #assert len(codes)==10
    print(len(codes))
    # uniqueness
    assert len(set(codes))==len(codes)
    # check one random TTWA that should be in there
    assert 'E31000198' in codes

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

def test_get_lsoa11_codes():
    # check there is not a more recent Census (2011 TTWAs are valid until 2021
    # included
    current_year = time.strftime("%Y")
    assert int(current_year)<=2021

    out = get_lsoa11_codes()
    assert isinstance(out, list)
    assert all(len(lsoa)==9 for lsoa in out)
    # uniqueness
    assert len(set(out)) == len(out)

def test_get_oa11_codes():
    current_year = time.strftime("%Y")
    assert int(current_year)<=2021

    out, _  = get_oa11_codes()
    assert isinstance(out,list)
    assert 'E00000001' in out
    assert len(out) == len(set(out)) == 181408

def test_ttwa11_to_lsoa11():
    # check there is not a more recent Census (2011 TTWAs are valid until 2021
    # included
    current_year = time.strftime("%Y")
    assert int(current_year)<=2021

    out = ttwa11_to_lsoa11(test=False)
    # all should be strings
    assert all(isinstance(ttwa,str) & isinstance(lsoa,str) for ttwa,lsoa in out)
    # all TTWA codes should be of length 9 (Note: LSOAs in NI are not)
    assert all(len(ttwa)==9 for ttwa,_ in out)
    # there should only be unique elements
    assert len(set(out)) == len(out)
    # assert I'm only picking up good ttwas
    ttwa11_list = get_ttwa11_codes()
    # check edimburgh is in there
    assert('S22000059' in ttwa11_list)
    assert all(ttwa in ttwa11_list for ttwa,_ in out)
    # hardcoded check - this should be a valid pair
    assert(('S22000059','S01008272') in out)

def test_lsoa11_to_oa11():
    # check there is not a more recent Census (2011 TTWAs are valid until 2021
    # included
    current_year = time.strftime("%Y")
    assert int(current_year)<=2021

    out_lsoa, out_msoa = lsoa11_to_oa11(test=False)
    # all should be strings
    assert all(isinstance(lsoa,str) & isinstance(oa,str) for lsoa,oa in out_lsoa)
    assert all(isinstance(msoa,str) & isinstance(oa,str) for msoa,oa in out_msoa)
    # all TTWA and LSOA codes should be of length 9 (CHANGE NAMES)
    assert all(len(lsoa)==9 & len(oa)==9 for lsoa,oa in out_lsoa)
    assert all(len(msoa)==9 & len(oa)==9 for msoa,oa in out_msoa)
    # there should only be unique elements
    assert len(set(out_lsoa)) == len(out_lsoa)
    assert len(set(out_msoa)) == len(out_msoa)
    # length should be the same because they need to match all OAs
    assert len(out_lsoa) == len(out_msoa)
    # assert I'm only picking up good LSOAs
    lsoa11_list = get_lsoa11_codes()
    assert all(lsoa in lsoa11_list for lsoa,_ in out_lsoa)
    # perhaps assert that I've got them all? i.e. all lsoa in lsoa11_list
    # check random pairs
    assert ('E01000001','E00000001') in out_lsoa
    assert ('E02000001', 'E00000003') in out_msoa

def test_get_oa_centroids():
    oa_centroids  = get_oa_centroids(n_start=0,n_end=100)
    assert isinstance(oa_centroids,list)
    assert(len(oa_centroids)==100)
    assert all([isinstance(t, tuple) for t in oa_centroids])


def test_compare_geographies():
    # ideally I would test that the LSOAs match between functions.
    # TODO later. It will not work until I get LSOAs and OAs for all UK
    assert 2==2
