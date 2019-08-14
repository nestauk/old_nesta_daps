"""
Finds the latest data-dump URL from Companies House, downloads it, and processes it.

TODO: Data dumps are available for previous months in a year too?
TODO: Deal with other columns
TODO: Merge with NSPL
"""
import datetime
import logging
import os

import engarde.checks as edc
import numpy as np
import pandas as pd
import requests


def _data_dump_url(date: datetime) -> str:
    """ Return data dump url based on current date """
    date = date.replace(day=1).strftime("%Y-%m-%d")  # Must be first day of month
    url = f"http://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-{date}.zip"
    logging.info(f"Companies house data-dump url: {url}")
    return url


def download_data_dump(date: datetime, cache: bool = True, nrows: int = None):
    """ Retrieve the data dump from companies house (or cached download if it exists)

    Args:
        date (`datetime`): Date-time of the month for the data-dump to get.
        cache (`bool`): If True, read from cache file if it exists or fetch and
            save to cache if it doesn't. If False don't read/write from cache file.
        nrows (`int`, optional): Number of rows to read

    Returns:
        pandas.DataFrame
    """
    cache_path = (
        f"/tmp/companies_house_data_dump_{date.strftime('%Y-%m-%d').replace('-', '_')}"
    )

    if os.path.isfile(cache_path) and cache:
        logging.info("Loading cached data dump")
    else:
        r = requests.get(_data_dump_url(date), allow_redirects=True)
        r.raise_for_status()
        if cache:
            with open(cache_path, "wb") as f:
                f.write(r.content)

    return pd.read_csv(cache_path, compression="zip", nrows=nrows)


def clean_ch(df):
    """ Clean and process Companies House data dump

    Args:
        df (`pandas.DataFrame`): Raw companies house data

    Returns:
        pandas.DataFrame
    """

    usecols = ["CompanyName", " CompanyNumber", "URI", "CompanyStatus"]
    newcols = ["company_name", "company_number", "URI", "company_status"]
    dtypes = [object, object, object, object, object]

    return (
        df.loc[:, usecols]
        .rename(columns={k: v for k, v in zip(usecols, newcols)})
        .dropna(subset=["company_number", "company_name"])
        .drop_duplicates("company_number")
        # TESTS
        .pipe(edc.is_shape, (None, 4))
        .pipe(edc.has_dtypes, items={k: v for k, v in zip(newcols, dtypes)})
    )


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(
        handlers=[log_stream_handler],
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(message)s",
    )

    date = datetime.datetime.now()
    print(download_data_dump(date).pipe(clean_ch).head())
