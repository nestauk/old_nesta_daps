"""
Query Companies House API with possibly valid company numbers to get the full population of company numbers.

TODO Chunk candidates update
TODO Stream chunks to file
"""
import asyncio
import datetime
import logging
import os
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


async def dispatcher(candidates, api_key):
    """ Queries Companies House API for candidates

    Args:
        candidates (`list` of `str`): Company numbers to checks
        api_key (`str`): API key for Companies House

    Returns:
        `list` of `dict`
    """
    logger.info(f"{len(candidates)} candidate company numbers.")
    async with ClientSession() as session:
        tasks = (
            try_company_number(session, number, BasicAuth(api_key))
            for number in candidates
        )
        out = await asyncio.gather(*tasks)
        logger.info("Completed tasks")
        n_found = np.sum([success for success, _ in out])
        logger.info(f"FOUND {n_found} dissolved companies")
    return out


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
    api_key = os.environ["CH_API_KEY"]
    print(api_key)

    candidates = ["01800000", "02800000"]

    loop = asyncio.get_event_loop()
    out = loop.run_until_complete(dispatcher(candidates, api_key))
    print(out)
