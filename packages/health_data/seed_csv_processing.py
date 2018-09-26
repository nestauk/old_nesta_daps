# import boto3
from datetime import datetime
import logging
import pandas as pd
import re
import requests
import time

from world_reporter import get_csv_data

log_stream_handler = logging.StreamHandler()
log_file_handler = logging.FileHandler('logs.log')
logging.basicConfig(handlers=(log_stream_handler, log_file_handler),
                    # level=logging.ERROR,
                    level=logging.INFO,
                    format="%(asctime)s:%(levelname)s:%(message)s")


def extract_year(date):
    '''
    Use a regex search for 4 digits in a row to identify the year.

    Args:
        date (str): The full date string.
    '''
    try:
        year = re.search(r'\d{4}', date).group(0)
    except (TypeError, AttributeError):
        logging.info(f"No year extraction possible for: {date}")
        return None

    return f"{year}-01-01"


def extract_date(date):
    '''
    Determine the date format, convert and return in YYYY-MM-DD format.

    Args:
        date (str): the full date string.
    '''
    date = date.strip()
    date_object = None
    date_formats = [
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

    for dateformat in date_formats:
        try:
            date_object = datetime.strptime(date, dateformat)
            break
        except ValueError:
            pass

    if not date_object:
        logging.info(f"No date conversion possible for: {date}")
        raise ValueError(f"Invalid date: {date}")

    return date_object.strftime('%Y-%m-%d')


def geocode(query=None, city=None, country=None):
    '''
    Geocoder using the Open Street Map Nominatim API.

    If there are multiple results the first one is returned (they are ranked by importance).
    The API usage policy allows maximum 1 request per second and no multithreading:
    https://operations.osmfoundation.org/policies/nominatim/

    Args:
        query (str): query string, multiple words should be separated with +
        city (str): name of the city.
        country (str): name of the country.
    '''
    if city and country:
        url = f"https://nominatim.openstreetmap.org/search?city={city}&country={country}&format=json"
    elif query:
        url = f"https://nominatim.openstreetmap.org/search?q={query}&format=json"
    else:
        raise TypeError("Missing argument: query or city and country required")

    headers = {'User-Agent': 'Nesta health data geocode'}
    response = requests.get(url, headers=headers)
    geo_data = response.json()
    if len(geo_data) < 1:
        return None
    else:
        lat = geo_data[0]['lat']
        lon = geo_data[0]['lon']
        return [lat, lon]


if __name__ == "__main__":
    df = get_csv_data()
    # TODO: convert cities and countries to lowercase to further reduce dupes
    deduped_locations = df[['city', 'country']].drop_duplicates()
    deduped_locations['coordinates'] = None

    failed_geocode_idx = []
    for idx, row in deduped_locations.iterrows():
        try:
            coordinates = geocode(city=row['city'], country=row['country'])
        except requests.exceptions.RequestException as e:
            failed_geocode_idx.append(idx)
            logging.error(f"id {idx} failed to geocode {row['city']}:{row['country']}")
            logging.exception(e)
        else:
            row['coordinates'] = coordinates
            logging.info(f"coordinates for {row['city']}: {coordinates}")
        finally:
            time.sleep(1)  # respect the OSM api usage limits

        # testing
        if idx > 12:
            break
    deduped_locations.to_csv('geocoded_cities.csv')  # just in case...

    from IPython import embed; embed()
    df = pd.merge(df, deduped_locations, how='left', left_on=['city', 'country'], right_on=['city', 'country'])
    print(df)
    # retry the failures with the query approach?...
    # also perform the cleaning for start and end dates while iterating through
