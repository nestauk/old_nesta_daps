import ast
from geopy.geocoders import GoogleV3
import logging
from pycountry import countries as pycountries
import requests
from retrying import retry
import time


@retry(wait_fixed=10000, stop_max_attempt_number=5)
def get_country_info(country):
    '''Call GoogleV3().geocode with retrying'''
    return GoogleV3().geocode(country)


class MeetupCountryGroups:
    '''Extract all meetup groups for a given country.

    Attrs:
        country_code (str): 2-letter country code assigned by `pycountry`.
        params (dict): GET request parameters, including lat/lon.
        groups (list): List of meetup groups in this country, assigned
            assigned after calling `get_groups`.
    '''

    def __init__(self, country, category, api_key):
        '''Set meetup search parameters.

        Args:
            country (str): A country name, which must exist in
                to pycountry.countries
        '''
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
        '''Recursively get all groups for the given parameters.
        It is assumed that you will run with the default arguments,
        since they are set automatically in the recursing procedure.
        '''
        
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


def flatten_data(mcg, desired_keys):
    '''Flatten the nested JSON data from :code:`mcg` by a
    list of predefined keys. Each element in the list
    may also be an ordered list of keys,
    such that subsequent keys describe a path through the
    JSON to desired value. For example in order to extract 
    `key1` and `key3` from:

    .. code-block:: python

        {'key': <some_value>, 'key2' : {'key3': <some_value>}}
    
    one would specify :code:`desired_keys` as:

    .. code-block:: python

        ['key1', ['key2', 'key3']]    

    Args:
        mcg (:obj:`MeetupCountryGroups`): A :code:`MeetupCountryGroups` object to be subsetted.
        desired_keys (:obj:`list`): Mixed list of either: individual `str` keys for data values
        which are not nested; **or** sublists of `str`, as described above.

    Returns:
       :obj:`list` of :obj:`dict`
    '''
    # Loop through groups
    group_info = []
    for info in mcg.groups:
        row = dict(urlname=info["urlname"], 
                   country_name=mcg.country_name)
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
    return group_info


if __name__ == "__main__":
    import sys  # For the API key

    # Get groups for this countru
    mcg = MeetupCountryGroups(country="Mexico",
                              category=34,
                              api_key=sys.argv[1])
    mcg.get_groups()
    logging.info("Got %s groups", len(mcg.groups))

    # Flatten the json data
    flatten_data(mcg, desired_columns=[('category', 'name'),
                                       ('category', 'shortname'),
                                       ('category', 'id'), 
                                       'description', 
                                       'created',
                                       'country',
                                       'city',
                                       'id',
                                       'lat',
                                       'lon',
                                       'members',
                                       'name',
                                       'topics'])
