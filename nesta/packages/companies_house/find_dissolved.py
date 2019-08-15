"""
Query Companies House API with possibly valid company numbers to get the full population of company numbers.

TODO Retries
TODO Account for bad server responses
"""
import asyncio
import datetime
import logging
import os
import time
from collections import namedtuple

import engarde.checks as edc
import numpy as np
import pandas as pd
import requests
from aiohttp import BasicAuth, ClientSession

logger = logging.getLogger(__name__)


async def try_company_number(session, company_number, api_key):
    """ Establish whether a company number exists or not

    Args:
        session ():
        company_number (`str`): Company Number to checks
        api_key (`str`): API key for Companies House

    Returns:
        (`bool`, `dict`)
    """
    logger.debug(f"submitted {company_number}")
    MockStatus = namedtuple("MockStatus", ["status", "reason"])
    try:
        url = f"https://api.companieshouse.gov.uk/company/{company_number}"
        r = await session.request(method="GET", url=url, auth=api_key)
        r.raise_for_status()
    except asyncio.TimeoutError:
        logger.debug("Timeout")
        r = MockStatus(reason="Request Timeout", status=408)
    except:
        if r.reason != "Not Found":
            logger.error(
                "Raised for bad status with unchecked reason:"
                f" {company_number}; {r.status}; {r.reason}"
            )
        else:
            pass

    logger.debug(f"RETURNING: {company_number} [{r.status}, {r.reason}]")
    if r.status == 200:
        body = await r.json()
        success = True
    else:
        body = []
        success = False

    return (
        success,
        {
            "company_number": company_number,
            "status": r.status,
            "reason": r.reason,
            "response": body,
        },
    )


async def dispatcher(candidates, api_key, consume, ratelim):
    """ Queries Companies House API for candidates

    Args:
        candidates (`list` of `str`): Company numbers to checks
        api_key (`str`): API key for Companies House
        consumer (`object`, optional): A method that consumes the output of
             `try_company_number`, e.g. `print` or a call to a database.
        ratelim (`tuple`): Max number of API calls (first element) in a given 
            number of seconds (second element).
    """
    API_CALLS, API_TIME = ratelim
    logger.info(f"{len(candidates)} candidate company numbers.")
    async with ClientSession() as session:
        sleep_time = 0
        while len(candidates):
            time.sleep(sleep_time)  # Wait until ratelim recovers

            tasks = [
                try_company_number(session, candidates.pop(), BasicAuth(api_key))
                for _ in range(min(API_CALLS, len(candidates)))
            ]
            for task in asyncio.as_completed(tasks):
                consume(await task)
            sleep_time = API_TIME


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

    loop = asyncio.get_event_loop()
    loop.run_until_complete(dispatcher(candidates, api_key, consume=print, ratelim=(API_CALLS, API_TIME)))
