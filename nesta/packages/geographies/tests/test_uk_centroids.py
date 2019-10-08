import sys
sys.path.insert(1,'/Users/stefgarasto/Google Drive/Documents/scripts/193_ttwa_travel/nesta')
from unittest import mock
import pytest
from nesta.packages.geographies.uk_centroids import get_ttwa_codes
from nesta.packages.geographies.uk_centroids import ttwa_to_lsoa
from nesta.packages.geographies.uk_centroids import lsoa_to_oa
from nesta.packages.geographies.uk_centroids import get_ttwa_name_from_id
from nesta.packages.geographies.uk_centroids import get_ttwa_id_from_name

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



# need to understand what mock.patch does!!!
mocked_find_filepath_from_pathstub = mock.patch(
    "nesta.packages.geographies.uk_centroids.find_filepath_from_pathstub")
mocked_find_filepath_from_pathstub.return_value=None
@mock.patch("builtins.open", new_callable=mock.mock_open, read_data=SPARQL_QUERY)
def test_get_ttwa_codes(mocked_open, mocked_find_filepath_from_pathstub):
    codes = get_ttwa_codes(test=True)
    assert isinstance(codes, list) # don't understand why len > 100?


def test_get_ttwa_to_lsoa():
    # placeholder
    assert 2==2

def test_get_lsoa_to_oa():
    # placeholder
    assert 2==2
