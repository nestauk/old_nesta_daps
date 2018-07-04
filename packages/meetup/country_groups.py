import ast
from geopy.geocoders import GoogleV3
import logging
from pycountry import countries as pycountries
import requests
from retrying import retry
import time

from utils.common.datapipeline import DataPipeline


@retry(wait_fixed=10000, stop_max_attempt_number=5)
def get_country_info(country):
    """Call geocode with retrying"""
    return GoogleV3().geocode(country)


class MeetupCountryGroups:
    """Extract all meetup groups for a given country.

    Attributes:
        country_code (str): 2-letter country code assigned by `pycountry`.
        params (dict): GET request parameters, including lat/lon.
        groups (list): List of meetup groups in this country, assigned
            assigned after calling `get_groups`.
    """
    def __init__(self, country, category, api_key):
        """MeetupCountryGroups constuctor.

        Args:
            country (str): A country name, which must exist in
                to pycountry.countries
        """
        # Retrieve country lat/lon and 2-letter country code
        self.country_code = pycountries.get(name=country).alpha_2
        self.country_name = country
        geo_info = get_country_info(country)
        # Assign request parameters
        self.params = dict(country=self.country_code,
                           key=api_key,
                           radius=800,
                           lat=geo_info.latitude,
                           lon=geo_info.longitude,
                           category=category,
                           page=200)
        print("Generated parameters", self.params)
        self.groups = []

    def get_groups(self, offset=0, max_pages=None):
        # Check if we're in too deep
        if max_pages is not None and offset >= max_pages:
            return
        # Set the offset parameter and make the request
        self.params["offset"] = offset
        r = requests.get("https://api.meetup.com/2/groups",
                         params=self.params)
        r.raise_for_status()
        # If no response is found
        if len(r.text) == 0:
            time.sleep(5)
            print("Got a bad response, so retrying page", offset)
            return self.get_groups(offset=offset, max_pages=max_pages)
        # Extract results in the country of interest (bonus countries
        # can enter the fold because of the radius parameter)
        data = r.json()
        for row in data["results"]:
            if row["country"].lower() != self.country_code.lower():
                continue
            if 'category' not in row:
                continue
            if str(row['category']['id']) != self.params['category']:
                continue
            self.groups.append(row)
        # Check if a "next" url is specified
        next_url = data["meta"]["next"]
        if next_url != "":
            # If so, increment offset and get the groups
            self.get_groups(offset=offset+1, max_pages=max_pages)


def run(config):
    mcg = MeetupCountryGroups(country=config['parameters']['country'],
                              category=config['parameters']['category'],
                              api_key=config['Meetup']['api-key'])
    mcg.get_groups()
    logging.info("Got %s groups", len(mcg.groups))

    # A list of field names to extract from the data
    desired_keys = ast.literal_eval(config['parameters']['desired_keys'])
    # Loop through groups
    group_info = []
    for info in mcg.groups:
        row = dict(urlname=info["urlname"], country_name=mcg.country_name)
        # Generate the field names and values, if they exist
        for key in desired_keys:
            field_name = key
            try:
                # If the key is just a string
                if type(key) == str:
                    value = info[key]
                # Otherwise, assume its a list of keys
                else:
                    field_name = "_".join(key)
                    # Recursively assign the list of keys
                    value = info
                    for k in key:
                        value = value[k]
            # Ignore fields which aren't found (these will appear
            # as NULL in the database anyway)
            except KeyError:
                continue
            row[field_name] = value
        group_info.append(row)

    with DataPipeline(config) as dp:
        for row in group_info:
            dp.insert(row)
