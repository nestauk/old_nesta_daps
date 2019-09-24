import datetime
import zipfile
from unittest import mock

import pandas as pd
import pytest
import requests

from nesta.packages.companies_house.collect_ch_data_dump import (
    clean_ch, download_data_dump)


@pytest.fixture()
def raw_data():
    return pd.DataFrame(
        [
            ["010000", "ACME Corp", "https://google.com", "Active"],
            ["010001", "ACME Corp2", "https://bing.com", "Active"],
        ],
        columns=["CompanyName", " CompanyNumber", "URI", "CompanyStatus"],
    )


@mock.patch("requests.get")
def test_download_data_dump(mock_requests):

    # Too large a download - check if it exists by looking at headers
    mock_requests.side_effect = requests.head
    good_date = datetime.datetime.now().replace(day=2)
    # The headers are not a zipped csv file so we expect an error here
    with pytest.raises(zipfile.BadZipFile):
        download_data_dump(good_date, cache_path="/tmp")

    bad_dates = [datetime.datetime.now().replace(year=2018)]
    for date in bad_dates:
        with pytest.raises(requests.HTTPError):
            download_data_dump(date)


def test_clean_ch(raw_data):
    clean_ch(raw_data)
