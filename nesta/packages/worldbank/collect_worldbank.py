"""
Collect worldbank
=================

Collect worldbank sociodemographic data by country.
"""

import requests
# from retrying import retry
import json
from collections import defaultdict
import re

WORLDBANK_ENDPOINT = "http://api.worldbank.org/v2/{}"
DEAD_RESPONSE = (None, None)  # tuple to match the default python return type


def worldbank_request(suffix, page, per_page=10000, data_key_path=None):
    """Hit the worldbank API and extract metadata and data from the response.

    Args:
        suffix (str): Suffix to append to :obj:`WORLDBANK_ENDPOINT`.
        page (int): Pagination number in API request.
        per_page (int): Number of results to return per request.
        data_key_path (list): List specifying json path to data object.
    Returns:
        metadata, data (dict, list): Metadata and data from API response.
    """
    response = _worldbank_request(suffix=suffix, page=page, per_page=per_page)
    metadata, data = data_from_response(response=response,
                                        data_key_path=data_key_path)
    return metadata, data


def _worldbank_request(suffix, page, per_page):
    """Hit the worldbank API and return the response.

    Args:
        suffix (str): Suffix to append to :obj:`WORLDBANK_ENDPOINT`.
        page (int): Pagination number in API request.
        per_page (int): Number of results to return per request.
    Returns:
        response (:obj:`requests.Response`)
    """
    # Hit the API
    r = requests.get(WORLDBANK_ENDPOINT.format(suffix),
                     params=dict(per_page=per_page, format="json", page=page))

    # There are some non-404 status codes which indicate invalid API request
    if r.status_code == 400:
        return DEAD_RESPONSE
    r.raise_for_status()

    # There are even some 200 status codes which indicate invalid API request
    # purely by returning non-json data
    response = DEAD_RESPONSE
    try:
        response = r.json()
    except json.JSONDecodeError:
        pass
    finally:
        return response


def data_from_response(response, data_key_path=None):
    """Split up the response from the worldbank API.

    Args:
        suffix (str): Suffix to append to :obj:`WORLDBANK_ENDPOINT`.
        data_key_path (list): List specifying json path to data object.
    Returns:
        metadata, data (dict, list): Metadata and data from API response.
    """
    # If the data is stored ({metadata}, [datarows])
    if data_key_path is None or response == DEAD_RESPONSE:
        metadata, datarows = response
    # Otherwise if the data is stored as {metadata, path:{[to:data]}}
    # (or similar)
    else:
        metadata = response
        datarows = response.copy()
        for key in data_key_path:
            datarows = datarows[key]
            if key != data_key_path[-1] and type(datarows) is list:
                datarows = datarows[0]
    return metadata, datarows


def worldbank_data(suffix, data_key_path=None):
    """Yield a row of data from worldbank API.

    Args:
        suffix (str): Suffix to append to :obj:`WORLDBANK_ENDPOINT`.
        data_key_path (list): List specifying json path to data object.
    Yields:
        row (dict): A row of data from the worldbank API.
    """
    # Discover the shape of the data by inspecting the metadata with
    # a tiny request (1 result, 1 page)
    metadata, _ = worldbank_request(suffix=suffix, page=1, per_page=1,
                                    data_key_path=data_key_path)
    # If the request was invalid
    if metadata is None:
        return

    # Iterate through pages until done
    total = int(metadata["total"])
    n, page = 0, 1
    while n < total:
        # Get the data, and yield row by row
        _, datarows = worldbank_request(suffix=suffix, page=page,
                                        data_key_path=data_key_path)
        for row in datarows:
            yield row
        page += 1
        n += len(datarows)


def get_worldbank_resource(resource):
    """Extract and flatten all data for one worldbank resource.

    Args:
        resource (str): One of "countries", "series" or "source"
    Returns:
        collection (list): A list of resource data.
    """
    collection = []
    for row in worldbank_data(resource):
        # Flatten out any data stored by a key named "value"
        data = {}
        for k, v in row.items():
            if type(v) is dict:
                v = v["value"]
            data[k] = v
        collection.append(data)
    return collection


def get_variables_by_code(codes):
    """Discover all dataset locations for each variable id, by variable code.
    Note: one variable may exist in many datasets, which is handy in the case
    of missing data.

    Args:
        codes (list): The codes of all variables to be discovered.
    Returns:
        variables (dict): Mapping of variable id --> dataset names.
    """
    # Mapping variable id --> dataset names
    variables = defaultdict(list)
    sources = get_worldbank_resource("source")
    for source in sources:
        # Extract variables in this "source" (dataset)
        suffix = f"sources/{source['id']}/series/data"
        data = worldbank_data(suffix, data_key_path=["source", "concept",
                                                     "variable"])
        # Filter out variables that we don't want
        filtered_data = filter(lambda row: (row['id'] in codes), data)
        # Assign remaining datasets to this variable
        for row in filtered_data:
            variables[row['id']].append(source['id'])
    return variables


def unpack_quantity(row, concept, value):
    """Unpack row like {"variable": [{"concept":<concept>, <value>:_i_want_this_}]}

    Args:
        row (dict): Row of Worldbank API data.
        concept (str): The name of the dataset containing the variable.
        value (str): The name of the variable to unpack.
    Returns:
        A value.
    """
    for quantity in row['variable']:
        if quantity['concept'] == concept:
            return quantity[value]
    raise NameError(f"No item found in {row['variable']} with "
                    f"concept = {concept}")


def unpack_data(row):
    """Unpack an entire row of Worldbank API data.

    Args:
        row (dict): Row of Worldbank API data.
    Returns:
        country, variable, value: Country, variable id and data value.
    """
    country = unpack_quantity(row, 'Country', 'id')
    variable = unpack_quantity(row, 'Series', 'value')
    value = row['value']
    return country, variable, value


def get_country_data(variables, year=2010):
    """Extract data for specified variables for all available
    countries, in a specified year.

    Args:
        variables (dict): Mapping of variable --> dataset ids.
        year (int): Year of data to be extracted.
    Returns:
        country_data (dict): Mapping of country --> variable name --> value
    """
    # Iterate through datasets
    country_data = defaultdict(dict)
    for series, sources in variables.items():
        # The name of a given variable varies subtlely across multiple
        # datasets, so we extract the variable name the first time for
        # consistency across datasets.
        variable_name = None
        done_countries = set()
        for source in sources:
            suffix = (f"sources/{source}/country/all/"
                      f"series/{series}/time/YR{year}/data")
            data = worldbank_data(suffix, data_key_path=["source", "data"])
            for country, variable, value in map(unpack_data, data):
                if value is None:  # Missing data for this country
                    continue
                if country in done_countries:  # Already done this country
                    continue
                if variable_name is None:  # Only equals True the first time
                    variable_name = variable
                done_countries.add(country)
                country_data[country][variable_name] = value
    return country_data


def flatten_country_data(country_data, country_metadata):
    """Merge and flatten country data and metadata together.

    Args:
        country_data (dict): Mapping of country --> variable name --> value
        country_metadata (list): List of country metadata.
    Returns:
        flat_country_data (list): Flattened country data and metadata.
    """
    flat_country_data = [dict(**country_data[metadata['id']], **metadata)
                         for metadata in country_metadata
                         if metadata['id'] in country_data]
    return flat_country_data


def clean_variable_names(flat_country_data):
    """Clean variable names ready for DB storage in place.

    Args:
        flat_country_data (list): Flattened country data.
    """
    for row in flat_country_data:
        for k, v in row.items():
            # Only clean names containing spaces
            if " " not in k:
                continue
            # Lower, replace '%', remove non-alphanums and use '_'
            new_key = k.lower().replace("%", "pc")
            new_key = re.sub('[^0-9a-zA-Z]+', ' ', new_key)
            new_key = new_key.lstrip().rstrip().replace(" ", "_")
            # Edit in place
            row[new_key] = row.pop(k)


if __name__ == "__main__":
    variables = get_variables_by_code(["SP.RUR.TOTL.ZS", "SP.URB.TOTL.IN.ZS",
                                       "SP.POP.DPND", "SP.POP.TOTL",
                                       "SP.DYN.LE00.IN", "SP.DYN.IMRT.IN",
                                       "BAR.NOED.25UP.ZS",
                                       "BAR.TER.CMPT.25UP.ZS",
                                       "NYGDPMKTPSAKD",
                                       "SI.POV.NAHC", "SI.POV.GINI"])
    country_data = get_country_data(variables)
    country_metadata = get_worldbank_resource("countries")
    flat_country_data = flatten_country_data(country_data, country_metadata)
    clean_variable_names(flat_country_data)
