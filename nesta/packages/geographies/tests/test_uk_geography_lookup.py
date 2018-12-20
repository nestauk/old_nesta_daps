from unittest import mock
import pytest
from nesta.packages.geographies.uk_geography_lookup import get_gss_codes
from nesta.packages.geographies.uk_geography_lookup import get_children
from nesta.packages.geographies.uk_geography_lookup import _get_children

SPARQL_QUERY = '''
PREFIX entity: <http://statistics.data.gov.uk/def/statistical-entity#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?area_code
WHERE {
    ?area_entity entity:code ?area_code_entity;
    rdfs:label ?area_code .
    ?area_code_entity rdfs:label ?area_code_type;
    FILTER(SUBSTR(?area_code_type, 2, 2) > "01").
}
'''
    

@pytest.fixture
def pars_for_get_children():
    return dict(base="dummy", geocodes="dummy", max_attempts=3)

@pytest.fixture
def side_effect_for_get_children():
    return ([1, 2], [2, 3], ["A", 3], ["5", 4], [])

@mock.patch("nesta.packages.geographies.uk_geography_lookup.find_filepath_from_pathstub", return_value=None)
@mock.patch("builtins.open", new_callable=mock.mock_open, read_data=SPARQL_QUERY)
def test_get_gss_codes(mocked_open, mocked_find_filepath_from_pathstub):
    codes = get_gss_codes(test=True)
    assert len(codes) > 100

def test_get_children():
    x = _get_children("E04", "E08000001")
    assert len(x) > 0

@mock.patch("nesta.packages.geographies.uk_geography_lookup._get_children")
def test_get_children_max_out(mocked, pars_for_get_children):
    mocked.side_effect = ([], [], [], [], [])
    get_children(**pars_for_get_children)    
    assert mocked.call_count == pars_for_get_children["max_attempts"] + 1


@mock.patch("nesta.packages.geographies.uk_geography_lookup._get_children")
def test_get_children_totals(mocked, pars_for_get_children, side_effect_for_get_children):
    mocked.side_effect = side_effect_for_get_children
    children = get_children(**pars_for_get_children)
    assert len(children) == sum(len(x) for x in side_effect_for_get_children)


