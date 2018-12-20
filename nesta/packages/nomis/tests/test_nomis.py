import pytest
from unittest import mock
import datetime
import pandas as pd
import os

from nesta.packages.nomis.nomis import get_base
from nesta.packages.nomis.nomis import get_code_pairs
from nesta.packages.nomis.nomis import reformat_nomis_columns
from nesta.packages.nomis.nomis import jaccard_repeats
from nesta.packages.nomis.nomis import discovery_iter
from nesta.packages.nomis.nomis import find_geographies
from nesta.packages.nomis.nomis import process_config

@pytest.fixture
def code_pairs():
    return {"something_code": "something_name",
            "other_code" : "other_name"}

@pytest.fixture
def columns():
    return ["something_name", "something_code",
            "other_name", "other_code", "final_code"]
    
@pytest.fixture
def df(columns):
    data = [{col: [0, 1, 2] for col in columns}
            for i in range(10)]
    for row in data:
        row["date"] = datetime.datetime(2018, 2, 1)
    return pd.DataFrame(data)    

@pytest.fixture
def dummy_row():
    return [{'TypeName': 'test', 'nomis_id': 1}]

def test_get_base():
    x = "something"
    assert get_base(f"{x}_code") == x

def test_get_code_pairs(code_pairs, columns):
    assert get_code_pairs(columns) == code_pairs

@mock.patch("nesta.packages.nomis.nomis.get_code_pairs")
def test_reformat_nomis_columns(mocked, code_pairs, df):
    mocked.return_value = code_pairs
    tables = reformat_nomis_columns({"sometable": df})
    assert len(tables) == 3
    for k, v in tables.items():
        assert len(v) == len(df)

def test_jaccard_repeats():
    list1 = ['dog', 'cat', 'rat', 'cat']
    list2 = ['dog', 'cat', 'rat']
    list3 = ['dog', 'cat', 'mouse']     
    assert jaccard_repeats(list1, list3) == 0.25
    assert jaccard_repeats(list2, list3) == 0.5

def test_discovery_iter():
    for row in discovery_iter("NM_1_1/geography."):
        assert 'name' in row
        assert 'nomis_id' in row            
        assert len(row) > 2

@mock.patch("nesta.packages.nomis.nomis.discovery_iter")
def test_successful_find_geographies(mocked, dummy_row):
    mocked.side_effect = lambda x: dummy_row
    assert find_geographies("test") == dummy_row


@mock.patch("nesta.packages.nomis.nomis.discovery_iter")
def test_failed_find_geographies(mocked, dummy_row):
    mocked.side_effect = lambda x: dummy_row
    with pytest.raises(ValueError) as err:
        find_geographies("_test_")

@mock.patch("nesta.packages.nomis.nomis.find_geographies")
def test_process_config(mocked, columns):
    mocked.side_effect = lambda x,y: columns
    this_dir = os.path.dirname(__file__)
    dirname = os.path.join(this_dir, '../../../production/config/official_data/')
    for filename in os.listdir(dirname):        
        if not filename.endswith(".config"):
            continue
        filename = filename.replace(".config", "")
        config, geogs_list, dataset_id, date_format = process_config(filename)
        for geogs in geogs_list:
            assert len(geogs) == len(columns)

        config, geogs_list, dataset_id, date_format = process_config(filename, test=True)
        for geogs in geogs_list:
            assert len(geogs) == 1
        
