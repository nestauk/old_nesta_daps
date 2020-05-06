"""
Query Companies House API with possibly valid company numbers
to get the full population of company numbers.

TODO Retries
TODO Account for bad server responses
"""
import datetime
import logging
import os
import time
from collections import namedtuple

import engarde.checks as edc
import numpy as np
import pandas as pd
import ratelim
import requests
from tenacity import retry, retry_if_result, stop_after_attempt

logger = logging.getLogger(__name__)

API_CALLS, API_TIME = 600, 300
MockResponse = namedtuple(
    "MockResponse", ["company_number", "status_code", "reason", "body"]
)


def _is_bad_status(value):
    """ Determines conditions for retrying a request """
    return value.status_code not in [200, 404]


def _return_bad_status(retry_state):
    """ Returns a MockResponse if retrying fails """
    r = retry_state.outcome.result()
    company_number = retry_state.args[1]
    return MockResponse(
        company_number=company_number,
        status_code=r.status_code,
        reason=r.reason,
        body="",
    )


@ratelim.patient(API_CALLS, API_TIME)
@retry(
    stop=stop_after_attempt(10),
    retry=retry_if_result(_is_bad_status),
    retry_error_callback=_return_bad_status,
)
def try_company_number(session, company_number):
    """ Establish whether a company number exists or not

    Args:
        session (requests.Session): requests session
        company_number (`str`): Company Number to checks

    Returns:
        MockResponse
    """
    print(company_number)
    logger.debug(f"submitted {company_number}")
    try:
        url = f"https://api.companieshouse.gov.uk/company/{company_number}"
        r = session.request(method="GET", url=url)
        r.company_number = company_number
        r.raise_for_status()
        r.body = r.json()
    except requests.exceptions.HTTPError:
        logger.debug(f"HTTPError: {r.status_code} {r.reason}")
        r.body = ""
        if r.status_code == 429:  # Too many requests
            logger.warning(f"429: Too many requests - waiting for {API_TIME} seconds")
            time.sleep(API_TIME * 1.1)
    # except requests.exceptions.ConnectionError:
    # except requests.exceptions.ConnectTimeout:  # 408
    logger.debug(f"RETURNING: {company_number} [{r.status_code}, {r.reason}]")

    return r


def dispatcher(candidates, api_key, consume):
    """ Queries Companies House API for candidates

    Args:
        candidates (`list` of `str`): Company numbers to checks
        api_key (`str`): API key for Companies House
        consumer (`object`, optional): A method that consumes the output of
             `try_company_number`, e.g. `print` or a call to a database.
    """
    logger.info(f"{len(candidates)} candidate company numbers.")
    with requests.Session() as session:
        session.auth = (api_key, "")
        [
            consume(try_company_number(session, candidates.pop()))
            for _ in range(len(candidates))
        ]


def generate_company_number_candidates(prefix: str):
    """ Generates potentially valid company numbers

    Args:
        prefix (`str`): Letter prefix

    Returns:
         `iter` of `str`
    """
    n_digits = 8 - len(prefix)
    for i in range(10 ** n_digits):
        yield prefix + str(i).zfill(n_digits)


if __name__ == "__main__":
    API_CALLS, API_TIME = 3, 3
    api_key = os.environ["CH_API_KEY"]

    # Generate some test numbers
    candidates = ["01800000", "02800000"]
    for c in candidates.copy():
        for i in range(1, 5):
            c = c[:-1] + str(i)
            candidates.append(c)

    dispatcher(candidates, api_key, consume=print)
