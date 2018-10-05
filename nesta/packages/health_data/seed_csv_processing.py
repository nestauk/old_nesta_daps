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
    Use search for 4 digits in a row to identify the year and return as YYYY-01-01.

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
        return None
    return date_object.strftime('%Y-%m-%d')


def fix_dates(df):
    '''
    A wrapper for the extract_date and extract_year functions to process a
    whole dataframe.

    Returns a dataframe with the original start_date and end_date columns
    appended with 'original' and the fixed dates taking the place of the
    original columns.

    Args:
        df (DataFrame): the pandas dataframe with the dates to be processed.
    '''
    df = df.rename(columns={'start_date': 'original_start_date',
                            'end_date': 'original_end_date'})
    df['start_date'], df['end_date'] = None, None
    for idx, row in df.iterrows():
        df.at[idx, 'start_date'] = extract_date(row.original_start_date) or extract_year(row.original_start_date)
        df.at[idx, 'end_date'] = extract_date(row.original_end_date) or extract_year(row.original_end_date)

    return df


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
        logging.error("Missing argument: query or city and country required")
        raise TypeError("Missing argument: query or city and country required")

    headers = {'User-Agent': 'Nesta health data geocode'}
    response = requests.get(url, headers=headers)
    geo_data = response.json()
    if len(geo_data) < 1:
        logging.debug(f"No geocode match for {query or (city, country)}")
        return None
    else:
        lat = geo_data[0]['lat']
        lon = geo_data[0]['lon']
        logging.debug(f"Successfully geocoded {query or (city, country)} to {lat, lon}")
        return {'lat': lat, 'lon': lon}


def geocode_dataframe(df, out_file='geocoded_cities.json', existing_file=None):
    '''
    A wrapper for the geocode function to process a supplied dataframe using
    the city and country.

    Returns a dataframe with a 'coordinates' column appended.

    Args:
        df (dataframe): a dataframe containing city and country fields.
        out_file (str): local file path to store the geocoded json.
        existing_file (str): path of local file that has previously been geocoded.
    '''
    if existing_file:
        deduped_locations = pd.read_json(existing_file, orient='records')
    else:
        deduped_locations = df[['city', 'country']].drop_duplicates()
        deduped_locations['coordinates'] = None

        for idx, row in deduped_locations.iterrows():
            try:
                coordinates = geocode(city=row['city'], country=row['country'])
                if coordinates is None:
                    time.sleep(1)
                    logging.info(f"retry {row['city']} with query method")
                    coordinates = geocode(f"{row['city']}+{row['country']}")
            except requests.exceptions.RequestException as e:
                logging.exception(e)
            else:
                deduped_locations.at[idx, 'coordinates'] = coordinates
                logging.info(f"coordinates for {row['city']}: {coordinates}")
            finally:
                time.sleep(1)  # respect the OSM api usage limits

        deduped_locations.to_json(out_file, orient='records')  # just in case...

    df = pd.merge(df, deduped_locations, how='left', left_on=['city', 'country'], right_on=['city', 'country'])

    return df


if __name__ == "__main__":
    df = get_csv_data()
    df = geocode_dataframe(df)
    df = fix_dates(df)
    df.to_csv('world_reporter_inputs_processed.csv')
