# import boto3
from datetime import datetime
import logging
# import pandas as pd
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

    for dateformat in ['%m/%d/%Y', '%Y/%m/%d', '%Y-%m-%d', '%b %d %Y', '%d %B %Y', '%B %Y', '%b %Y', '%Y']:
        try:
            date_object = datetime.strptime(date, dateformat)
            break
        except ValueError:
            pass

    if not date_object:
        raise ValueError(f"Invalid date: {date}")

    return date_object.strftime('%Y-%m-%d')


def geocode(city=None, country=None, query=None):
    '''
    Geocoder using the Open Street Map Nominatim API.

    API usage policy allows maximum 1 request per second and no multithreading.
    https://operations.osmfoundation.org/policies/nominatim/

    Args:
        city (str): name of the city.
        country (str): name of the country.
        query (str): query string, multiple words should be separated with +
    '''
    if city or country:
        url = f"https://nominatim.openstreetmap.org/search?city={city}&country={country}&format=json"
    elif query:
        url = f"https://nominatim.openstreetmap.org/search?q={query}&format=json"
    else:
        raise TypeError("Missing argument: city, country or query required")

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

    for idx, row in deduped_locations.iterrows():
        try:
            coordinates = geocode(city=row['city'], country=row['country'])
            row['coordinates'] = coordinates
        except requests.exceptions.RequestException as e:
            logging.error(f"id {idx} failed to geocode {row['city']}:{row['country']}")
            logging.exception(e)
        finally:
            logging.info(f"coordinates for {row['city']}: {coordinates}")
            time.sleep(1)  # respect the OSM api usage limits

        # testing
        if idx > 12:
            break
    print(deduped_locations)

    # now join the coordinates back to the original data
    # also perform the cleaning for start and end dates while iterating through
