"""
Collect worldbank
=================

Collect worldbank sociodemographic data by country.
"""

import requests
from retrying import retry
import json
from collections import defaultdict
import re
import math
import pandas as pd

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


@retry(stop_max_attempt_number=3, wait_fixed=2000)
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
        response (tuple): Response from worldbank API, expected to be a tuple of two json items.
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


def calculate_number_of_api_pages(suffix, per_page=10000, data_key_path=None):
    """Calculate the number of API scrolls required to paginate through this
    request.

    Args:
        suffix (str): Suffix to append to :obj:`WORLDBANK_ENDPOINT`.
        per_page (int): Number of results to return per request.
        data_key_path (list): List specifying json path to data object.
    Returns:
        n_pages (int): Number of API scrolls required.
    """
    # Discover the shape of the data by inspecting the metadata with
    # a tiny request (1 result, 1 page)
    metadata, _ = worldbank_request(suffix=suffix, page=1,
                                    per_page=1,
                                    data_key_path=data_key_path)

    # If the request was invalid, there are no pages
    if metadata is None:
        return 0

    # Calculate the number of pages required
    total = int(metadata["total"])
    n_pages = math.floor(total / per_page) + int(total % per_page > 0)
    return n_pages


def worldbank_data_interval(suffix, first_page, last_page,
                            per_page=10000, data_key_path=None):
    """Yield a row of data from worldbank API in a page interval.
    Args:
        suffix (str): Suffix to append to :obj:`WORLDBANK_ENDPOINT`.
        {first, last}_page (int): First (last) page number of the API request.
        per_page (int): Number of results to return per request.
        data_key_path (list): List specifying json path to data object.
    Yields:
        row (dict): A row of data from the worldbank API.
    """
    for page in range(first_page, last_page+1):
        _, datarows = worldbank_request(suffix=suffix, page=page,
                                        per_page=per_page,
                                        data_key_path=data_key_path)
        if datarows is None:
            continue
        for row in datarows:
            yield row


def worldbank_data(suffix, per_page=10000, data_key_path=None):
    """Yield a row of data from worldbank API in a page interval.
    Args:
        suffix (str): Suffix to append to :obj:`WORLDBANK_ENDPOINT`.
        per_page (int): Number of results to return per request.
        data_key_path (list): List specifying json path to data object.
    Yields:
        row (dict): A row of data from the worldbank API.
    """
    n_pages = calculate_number_of_api_pages(suffix=suffix,
                                            per_page=per_page,
                                            data_key_path=data_key_path)
    return worldbank_data_interval(suffix, first_page=1,
                                   last_page=n_pages,
                                   per_page=per_page,
                                   data_key_path=data_key_path)


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
    key_path = ["source", "concept", "variable"]

    # Mapping variable id --> dataset names
    variables = defaultdict(list)
    sources = get_worldbank_resource("source")
    for source in sources:
        # Extract variables in this "source" (dataset)
        suffix = f"sources/{source['id']}/series/data"
        data = worldbank_data(suffix, data_key_path=key_path)
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
        country, variable, time, value
    """
    country = unpack_quantity(row, 'Country', 'id')
    #variable = unpack_quantity(row, 'Series', 'value')
    time = unpack_quantity(row, 'Time', 'value')
    value = row['value']
    return country, time, value


def get_country_data(variables, aliases, time="all"):
    """Extract data for specified variables for all available
    countries, in a specified year.

    Args:
        variables (dict): Mapping of variable --> dataset ids.
        aliases (dict): Mapping of dirty -> clean variable name.
        time (str): String to identify time period to request.
    Returns:
        country_data (dict): Mapping of country --> variable name --> value
    """
    # Iterate through datasets
    country_data = defaultdict(dict)  # lambda: defaultdict(list))
    kwargs_list = get_country_data_kwargs(variables=variables,
                                          aliases=aliases,
                                          time=time)
    for kwargs in kwargs_list:
        _country_data = country_data_single_request(**kwargs)
        for country, data in _country_data.items():
            for var_name, data_row in data.items():
                country_data[country][var_name] = data_row
    return country_data


def get_country_data_kwargs(variables, aliases, time="all", per_page=10000, max_pages=None):
    """Generate every set of kwargs required to make single requests.
    Designed to be used with :obj:`country_data_single_request` in order
    to be batched.

    Args:
        variables (dict): Mapping of variable --> dataset ids.
        aliases (dict): Mapping of variable name aliases, to ensure
                        consistent variable naming between data collections.
        per_page (int): Number of results to return per request.
    Returns:
        kwargs_list (list): kwargs list for :obj:`country_data_single_request`.
    """
    # Iterate through datasets
    kwargs_list = []
    key_path = ["source", "data"]
    for series, sources in variables.items():
        # The name of a given variable varies subtlely across multiple
        # datasets, so we extract the variable name the first time for
        # consistency across datasets.
        alias = aliases[series]
        for source in sources:
            suffix = (f"sources/{source}/country/all/"
                      f"series/{series}/time/{time}/data")
            n_pages = calculate_number_of_api_pages(suffix=suffix,
                                                    per_page=per_page,
                                                    data_key_path=key_path)
            for page in range(1, n_pages+1):
                if max_pages is not None and page > max_pages:
                    break
                parameters = dict(alias=alias,
                                  suffix=suffix, first_page=page,
                                  last_page=page, per_page=per_page,
                                  data_key_path=key_path)
                kwargs_list.append(parameters)
    return kwargs_list


def country_data_single_request(alias, **kwargs):
    """Extract data for all countries using kwargs generated by
    :obj:`get_country_data_kwargs`.

    Args:
        kwargs (dict): An item returned by :obj:`get_country_data_kwargs`.
    Returns:
        country_data (dict): Mapping of country --> variable name --> value
    """
    country_data = defaultdict(lambda: defaultdict(list))
    done_pkeys = set()
    data = worldbank_data_interval(**kwargs)
    for country, time, value in map(unpack_data, data):
        if value is None:  # Missing data for this country
            continue
        pkey = (country, time)
        if pkey in done_pkeys:  # Already done this country
            continue
        done_pkeys.add(pkey)
        new_row = {"value": value, "time": time}
        country_data[country][alias].append(new_row)
    return country_data


# def flatten_country_data(country_data, country_metadata):
#     """Merge and flatten country data and metadata together.

#     Args:
#         country_data (dict): Mapping of country --> variable name --> value
#         country_metadata (list): List of country metadata.
#     Returns:
#         flat_country_data (list): Flattened country data and metadata.
#     """
#     flat_country_data = [dict(**country_data[metadata['id']], **metadata)
#                          for metadata in country_metadata
#                          if metadata['id'] in country_data]
#     return flat_country_data


def discover_variable_name(series):
    """Discover variable names from each series [short hand code].

    Args:
        series (str): The short hand code for the variable name,
                      according to Worldbank API.
    Returns:
        alias (str): The variable name for the given series.
    """
    _, data = worldbank_request(f"en/indicator/{series}", page=1)
    alias = data[0]["name"]
    return alias


def clean_variable_name(var_name):
    """Clean a single variable name ready for DB storage.

    Args:
        var_name (str): Variable name to be cleaned
    Returns:
        new_var_name (str): A MySQL compliant variable name.
    """

    # Lower, replace '%', remove non-alphanums and use '_'
    new_var_name = var_name.lower().replace("%", "pc")
    new_var_name = re.sub('[^0-9a-zA-Z]+', ' ', new_var_name)
    new_var_name = new_var_name.lstrip().rstrip().replace(" ", "_")
    # Recursively shorten from middle character until less than 64 chars long
    # (this is the default MySQL limit)
    # Middle character has been chosen to allow some readability.
    while len(new_var_name) > 64:
        # Find the longest term
        longest_term = ""
        for term in new_var_name.split("_"):
            if len(term) <= len(longest_term):
                continue
            longest_term = term
        # Remove the middle character from the longest term
        middle = len(longest_term) - 1
        new_term = longest_term[:middle] + longest_term[middle+1:]
        new_var_name = new_var_name.replace(longest_term, new_term)
    return new_var_name



def clean_variable_names(flat_country_data):
    """Clean variable names ready for DB storage.

    Args:
        flat_country_data (list): Flattened country data.
    Returns:
        out_data (list): Same as input data, with MySQL compliant field names.
    """
    out_data = []
    for row in flat_country_data:
        new_row = {}
        for key, v in row.items():
            # Only clean names containing spaces
            if " " not in key:
                new_row[key] = v
                continue
            new_key = clean_variable_name(key)
            new_row[new_key] = v
        out_data.append(new_row)
    return out_data


def is_bad_quarter(x, bad_quarters):
    return any(q in x for q in bad_quarters)


def flatten_country_data(country_data, country_metadata,
                         bad_quarters=("Q1", "Q3", "Q4")):
    # Discover the year from the time variable.
    # Expected to be in the formats "XXXX" and "XXXX QX"
    country_metadata = {metadata["id"]: metadata
                        for metadata in country_metadata}
    fairly_flat_data = []
    for iso3, _ in country_metadata.items():
        for variable, data in country_data[iso3].items():
            for row in data:
                year = re.findall('(\d{4})', row["time"])[0]
                flatter_row = dict(country=iso3, variable=variable,
                                   year=year, **row)
                fairly_flat_data.append(flatter_row)

    # Group by country and year and remove quarters we don't want.
    very_flat_data = []
    df = pd.DataFrame(fairly_flat_data)
    for (country, year), _df in df.groupby(["country", "year"]):
        # Identify quarters we don't want
        condition = _df["time"].apply(is_bad_quarter,
                                      bad_quarters=bad_quarters)
        if (~condition).sum() == 0:
            continue
        row = {r["variable"]: r["value"]
               for _, r in _df.loc[~condition].iterrows()}
        row = dict(year=year, **row, **country_metadata[country])
        very_flat_data.append(row)

    return very_flat_data


if __name__ == "__main__":

    # DO ALL OF THE FOLLOWING IN A SINGLE LUIGI TASK
    variables = get_variables_by_code(["SP.RUR.TOTL.ZS", "SP.URB.TOTL.IN.ZS",
                                       "SP.POP.DPND", "SP.POP.TOTL",
                                       "SP.DYN.LE00.IN", "SP.DYN.IMRT.IN",
                                       "BAR.NOED.25UP.ZS",
                                       "BAR.TER.CMPT.25UP.ZS",
                                       "NYGDPMKTPSAKD",
                                       "SI.POV.NAHC", "SI.POV.GINI"])
    aliases = {series: discover_variable_name(series)
               for series, sources in variables.items()}

    # (EXCEPT THIS ONE)
    # country_data = get_country_data(variables, aliases)

    # ALSO DO THIS
    kwargs_list = get_country_data_kwargs(variables=variables,
                                          aliases=aliases,
                                          max_pages=2)  # <== Test mode

    # THEN BATCH, FOR CHUNKS OF KWARGS
    country_data = defaultdict(dict)
    for kwargs in kwargs_list:
        _country_data = country_data_single_request(**kwargs)
        for country, data in _country_data.items():
            for var_name, data_row in data.items():
                country_data[country][var_name] = data_row


    # THEN A FINAL TASK GETS THE ABOVE RESULTS BY COUNTRY
    # AND WRITES TO DATABASE
    country_metadata = get_worldbank_resource("countries")
    flat_country_data = flatten_country_data(country_data, country_metadata)
    cleaned_data = clean_variable_names(flat_country_data)

    # STEP AFTER PUTS THESE IN ES
