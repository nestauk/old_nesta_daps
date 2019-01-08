'''
Process NIH
===========

Data cleaning and processing procedures for the NIH World Reporter data.
Specifically, a lat/lon is generated for each city/country; and
the formatting of date fields is unified.
'''

import logging
import pandas as pd

from nesta.packages.format_utils.datetools import extract_date
from nesta.packages.format_utils.datetools import extract_year
from nesta.packages.geo_utils.geocode import geocode_dataframe
from nesta.packages.geo_utils.country_iso_code import country_iso_code_dataframe


def _extract_date(date, date_format='%Y-%m-%d'):
    '''
    Extract the date if a valid format exists, otherwise just extract the year
    and return {year}-01-01.

    Args:
        date (str): Full date string, with unknown formatting
        date_format (str): Output format string

    Returns:
        :code:`str` of date formatted according to date_format.
        Returns :code:`None` if not even a year is found.
    '''
    # Try to extract the date if a valid format exists
    try:
        date = extract_date(date, date_format)
    except ValueError:
        pass
    else:
        return date
    # Otherwise extract the year
    try:
        year = extract_year(date)
    except ValueError:
        # Implies not even a year can be found
        return None
    else:
        # Default formatting if only a year is found
        return f'{year}-01-01'


if __name__ == "__main__":
    # Local imports and log settings
    from sqlalchemy.orm import sessionmaker
    from nesta.production.orms.orm_utils import get_mysql_engine
    from nesta.production.orms.world_reporter_orm import Abstracts

    log_stream_handler = logging.StreamHandler()
    log_file_handler = logging.FileHandler('logs.log')
    logging.basicConfig(handlers=(log_stream_handler, log_file_handler),
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")

    # Get first 30 rows, and strip off prefixes
    engine = get_mysql_engine("MYSQLDB", "mysqldb", "dev")
    cols = ['application_id', 'org_city', 'org_country',
            'project_start', 'project_end']
    df = pd.read_sql('nih_projects', engine, columns=cols).head(30)
    df.columns = [c.lstrip('org_') for c in df.columns]

    # For sense-checking later
    n = len(df)
    n_ids = len(set(df.application_id))

    # Geocode the dataframe
    df = geocode_dataframe(df)
    # clean start and end dates
    for col in ["project_start", "project_end"]:
        df[col] = df[col].apply(_extract_date)
    # append iso codes for country
    df = country_iso_code_dataframe(df)
    assert len(set(df.application_id)) == n_ids
    assert len(df) == n

    # Print the results
    for _, row in df.iterrows():
        logging.info(dict(row))
