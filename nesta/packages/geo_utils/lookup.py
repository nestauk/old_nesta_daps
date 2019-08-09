import requests
import pandas as pd

def get_continent_lookup():
    url = ("https://nesta-open-data.s3.eu-west"
           "-2.amazonaws.com/rwjf-viz/"
           "continent_codes_names.json")
    continent_lookup = {row["Code"]: row["Name"]
                        for row in requests.get(url).json()}
    continent_lookup[None] = None
    continent_lookup[''] = None
    return continent_lookup

def get_country_region_lookup():
    url = ("https://datahub.io/core/country-codes"
           "/r/country-codes.csv")
    df = pd.read_csv(url, usecols=['official_name_en', 'ISO3166-1-Alpha-2', 
                                   'Sub-region Name'])
    data = {row['ISO3166-1-Alpha-2']: (row['official_name_en'], 
                                       row['Sub-region Name']) 
            for _, row in df.iterrows() 
            if not pd.isnull(row['official_name_en'])
            and not pd.isnull(row['ISO3166-1-Alpha-2'])}
    data['XK'] = ('Kosovo', 'Southern Europe')    
    data['TW'] = ('Kosovo', 'Eastern Asia')
    return data
