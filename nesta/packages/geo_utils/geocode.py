'''
geocode
=======

Tools for geocoding.
'''

import logging
import pandas as pd
import requests
from retrying import retry
from functools import lru_cache

from nesta.packages.decorators.ratelimit import ratelimit


@lru_cache()
def geocode(**request_kwargs):
    '''
    Geocoder using the Open Street Map Nominatim API.

    If there are multiple results the first one is returned (they are ranked by importance).
    The API usage policy allows maximum 1 request per second and no multithreading:
    https://operations.osmfoundation.org/policies/nominatim/

    Args:
        request_kwargs (dict): Parameters for OSM API.
    Returns:
        JSON from API response.
    '''
    # Explictly require json for ease of use
    request_kwargs["format"] = "json"
    response = requests.get("https://nominatim.openstreetmap.org/search",
                            params=request_kwargs,
                            headers={'User-Agent': 'Nesta health data geocode'})
    response.raise_for_status()
    geo_data = response.json()
    if len(geo_data) == 0:
        raise ValueError(f"No geocode match for {request_kwargs}")
    return geo_data


def retry_if_not_value_error(exception):
    """Forces retry to exit if a valueError is returned. Supplied to the
    'retry_on_exception' argument in the retry decorator.

    Args:
        exception (Exception): the raised exception, to check

    Returns:
        (bool): False if a ValueError, else True
    """
    return not isinstance(exception, ValueError)


@retry(stop_max_attempt_number=10, retry_on_exception=retry_if_not_value_error)
@ratelimit(max_per_second=0.5)
def _geocode(q=None, **kwargs):
    '''Extension of geocode to catch invalid requests to the api and handle errors.
    failure.

    Args:
        q (str): query string, multiple words should be separated with +
        kwargs (str): name and value of any other valid query parameters

    Returns:
        dict: lat and lon
    '''
    valid_kwargs = ['street', 'city', 'county', 'state', 'country', 'postalcode']
    if not all(kwarg in valid_kwargs for kwarg in kwargs):
        raise ValueError(f"Invalid query parameter. Not in: {valid_kwargs}")
    if q and kwargs:
        raise ValueError("Supply either q OR other query parameters, they cannot be combined.")
    if not q and not kwargs:
        raise ValueError("No query parameters supplied")

    query_kwargs = {'q': q} if q else kwargs
    try:
        geo_data = geocode(**query_kwargs)
    except ValueError:
        logging.debug(f"Unable to geocode {query_kwargs}")
        return None  # converts to null which is accepted in elasticsearch

    lat = geo_data[0]['lat']
    lon = geo_data[0]['lon']
    logging.debug(f"Successfully geocoded {query_kwargs} to {lat, lon}")

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
    _df.replace('', pd.np.nan, inplace=True)
    _df = _df.dropna()
    if len(_df) == 0:        
        df[out_col] = None
        return df

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


def geocode_batch_dataframe(df, city='city', country='country',
                            latitude='latitude', longitude='longitude',
                            query_method='both'):
    """Geocodes a dataframe, first by supplying the city and country to the api, if this
    fails a second attempt is made supplying the combination using the q= method.
    The supplied dataframe df is returned with additional columns appended, containing
    the latitude and longitude as floats.

    Args:
        df (:obj:`pandas.DataFrame`): input dataframe
        city (str): name of the input column containing the city
        country (str): name of the input column containing the country
        latitude (str): name of the output column containing the latitude
        longitude (str): name of the output column containing the longitude
        query_method (int): query methods to attempt:
                                    'city_country_only': city and country only
                                    'query_only': q method only
                                    'both': city, country with fallback to q method

    Returns:
        (:obj:`pandas.DataFrame`): original dataframe with lat and lon appended as floats
    """
    if query_method not in ['city_country_only', 'query_only', 'both']:
        raise ValueError("Invalid query method, must be 'city_country_only', 'query_only' or 'both'")

    df[latitude], df[longitude] = None, None

    for idx, row in df.iterrows():
        location = None
        if query_method in ['city_country_only', 'both']:
            location = _geocode(city=row[city], country=row[country])
        if location is None and query_method in ['query_only', 'both']:
            query = f"{row[city]} {row[country]}"
            location = _geocode(q=query)
        if location is not None:
            df.loc[idx, latitude] = float(location['lat'])
            df.loc[idx, longitude] = float(location['lon'])
    return df


def generate_composite_key(city=None, country=None):
    """Generates a composite key to use as the primary key for the geographic data.

    Args:
        city (str): name of the city
        country (str): name of the country

    Returns:
        (str): composite key
    """
    try:
        city = city.replace(' ', '-').lower()
        country = country.replace(' ', '-').lower()
    except AttributeError:
        raise ValueError(f"Invalid city or country name. City: {city} | Country: {country}")
    return '_'.join([city, country])
