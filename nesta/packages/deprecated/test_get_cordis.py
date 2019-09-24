from unittest import mock
import pytest
import pandas as pd

from nesta.packages.cordis.get_cordis import TOP_URL
from nesta.packages.cordis.get_cordis import ENTITIES
from nesta.packages.cordis.get_cordis import fetch_and_clean

@pytest.fixture
def data():
    return pd.DataFrame([{'ColName': 10, 'AnotherCol': 20, 
                          'ThirdCol': None},
                         {'AnotherCol': 40, 'FinalCol': 'a thing'}]*1000)

def test_url():
    import requests
    for fp, entities in ENTITIES.items():
        for entity_name in entities:
            r = requests.head(TOP_URL.format(fp, entity_name))
            r.raise_for_status()

@mock.patch('nesta.packages.cordis.get_cordis.pd.read_csv')
def test_fetch_and_clean(mocked_read_csv, data):
    mocked_read_csv.return_value = data
    df = fetch_and_clean('fpXX', 'something')
    assert len(df) == len(data)
    assert len(df.columns) == len(data.dropna(axis=1, how='all').columns)
    assert all(col.lower() == col for col in df.columns)
