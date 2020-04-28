import requests
import pandas as pd
from io import StringIO

def get_eu_countries():
    """
    All EU ISO-2 codes
    
    Returns:
        data (list): List of ISO-2 codes)
    """
    url = 'https://restcountries.eu/rest/v2/regionalbloc/eu'
    r = requests.get(url)        
    return [row['alpha2Code'] for row in r.json()]


def get_continent_lookup():
    """
    Retrieves continent ISO2 code to continent name mapping from a static open URL.

    Returns:
        data (dict): Key-value pairs of continent-codes and names.
    """

    url = ("https://nesta-open-data.s3.eu-west"
           "-2.amazonaws.com/rwjf-viz/"
           "continent_codes_names.json")
    continent_lookup = {row["Code"]: row["Name"]
                        for row in requests.get(url).json()}
    continent_lookup[None] = None
    continent_lookup[''] = None
    return continent_lookup

def get_country_region_lookup():
    """
    Retrieves subregions (around 18 in total)
    lookups for all world countries, by ISO2 code,
    form a static open URL.

    Returns:
        data (dict): Values are country_name-region_name pairs.
    """
    url = ("https://datahub.io/core/country-codes"
           "/r/country-codes.csv")
    r = requests.get(url)
    r.raise_for_status()
    with StringIO(r.text) as csv:        
        df = pd.read_csv(csv, usecols=['official_name_en', 'ISO3166-1-Alpha-2',
                                       'Sub-region Name'])
    data = {row['ISO3166-1-Alpha-2']: (row['official_name_en'],
                                       row['Sub-region Name'])
            for _, row in df.iterrows()
            if not pd.isnull(row['official_name_en'])
            and not pd.isnull(row['ISO3166-1-Alpha-2'])}
    data['XK'] = ('Kosovo', 'Southern Europe')
    data['TW'] = ('Kosovo', 'Eastern Asia')
    return data
