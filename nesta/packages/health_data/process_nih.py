'''
Process NIH
===========

Data cleaning and processing procedures for the NIH World Reporter data.
Specifically, a lat/lon is generated for each city/country; and
the formatting of date fields is unified.
'''

import logging
import pandas as pd
from retrying import retry
from nesta.packages.decorators.ratelimit import ratelimit
from nesta.packages.format_utils.datetools import extract_date
from nesta.packages.format_utils.datetools import extract_year
from nesta.packages.geo_utils.geocode import geocode


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


@retry(stop_max_attempt_number=10)
@ratelimit(max_per_second=1)
def _geocode(q=None, city=None, country=None):
    '''
    Args:
        q (str): query string, multiple words should be separated with +
        city (str): name of the city.
        country (str): name of the country.
    Returns:
        dict of lat and lon.
    '''
    if city and country:
        geo_data = geocode(country=country, city=city)
    elif q and not (city or country):
        geo_data = geocode(q=q)
    else:
        raise TypeError("Missing argument: q or city and country required")

    lat = geo_data[0]['lat']
    lon = geo_data[0]['lon']
    logging.debug(f"Successfully geocoded {q or (city, country)} to {lat, lon}")
    return {'lat': lat, 'lon': lon}


def geocode_dataframe(df):
    '''
    A wrapper for the geocode function to process a supplied dataframe using
    the city and country.

    Args:
        df (dataframe): a dataframe containing city and country fields.
    Returns:
        a dataframe with a 'coordinates' column appended.
    '''
    in_cols = ['city', 'country']
    out_col = 'coordinates'
    # Only geocode unique city/country combos
    _df = df[in_cols].drop_duplicates()
    # Attempt to geocode with city and country
    _df[out_col] = _df[in_cols].apply(lambda row: _geocode(**row), axis=1)
    # Attempt to geocode with query for those which failed
    null = pd.isnull(_df[out_col])
    if null.sum() > 0:
        query = "{city} {country}"
        _df.loc[null, out_col] = _df.loc[null, in_cols].apply(lambda row:
                                                              _geocode(query.format(**row)),
                                                              axis=1)
    # Merge the results again
    return pd.merge(df, _df, how='left', left_on=in_cols, right_on=in_cols)


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
    for col in ["project_start", "project_end"]:
        df[col] = df[col].apply(_extract_date)
    assert len(set(df.application_id)) == n_ids
    assert len(df) == n
        
    # Print the results
    for _, row in df.iterrows():
        logging.info(dict(row))
