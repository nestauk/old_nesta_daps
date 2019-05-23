'''
datetools
=========

Tools for processing dates in data.
'''

from datetime import datetime as dt
import re

DATE_FORMATS = [
    '%m/%d/%Y',   # 10/15/2013
    '%Y/%m/%d',   # 2013/10/15
    '%Y-%m-%d',   # 2013-10-15
    '%b %d %Y',   # Oct 15 2013
    '%d %B %Y',   # 15 October 2013
    '%d %b, %Y',  # 15 Oct, 2013
    '%B %Y',      # October 2013
    '%b %Y',      # Oct 2013
    '%Y'          # 2013
]


def extract_year(date):
    '''
    Use search for 4 digits in a row to identify the year and return as YYYY-01-01.

    Args:
        date (str): The full date string.

    Returns:
        integer
    '''
    try:
        year = re.search(r'\d{4}', date).group(0)
    except (TypeError, AttributeError):
        raise ValueError(f"No year extraction possible for: {date}")
    return int(year)


def extract_date(date, date_format='%Y-%m-%d', return_date_object=False):
    '''
    Determine the date format, convert and return in YYYY-MM-DD format.

    Args:
        date (str): the full date string.
    Returns:
        Formatted date string.
    '''
    date_object = None
    for df in DATE_FORMATS:
        try:
            date_object = dt.strptime(date.strip(), df)
            break
        except (ValueError, AttributeError):
            pass

    if not date_object:
        raise ValueError(f"No date conversion possible for: {date}")

    if return_date_object:
        return date_object
    return date_object.strftime(date_format)
