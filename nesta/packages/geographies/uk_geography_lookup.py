import warnings
warnings.warn("UK Geographies is deprecated, "
              "and needs fixing, but there are currently "
              "no dependencies", DeprecationWarning)
raise Exception

from nesta.packages.misc_utils.sparql_query import sparql_query
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub
from collections import defaultdict
import requests


BASE_URL = "http://statistics.data.gov.uk/area_collection.json"
COLLECTION = "http://statistics.data.gov.uk/def/geography/collection/{}"
WITHIN = "http://statistics.data.gov.uk/id/statistical-geography/{}"
OFFICIAL_NAME = "http://statistics.data.gov.uk/def/statistical-geography#officialname"
ENDPOINT = "http://statistics.data.gov.uk/sparql"


def _get_children(base, geocode):
    """Hit the hidden ONS/ESRI API in order to reveal any nested geographies.

    Args:
        base (str): GSS 3-digit base code of target (child) geographies.
        geocode (str): GSS 9-digit code of test (parent) geography.
    Returns:
        List of dict of children.
    """
    params=dict(in_collection=COLLECTION.format(base),
                within_area=WITHIN.format(geocode),
                per_page=100000)
    r = requests.get(BASE_URL, params=params)
    print(r.text)
    children = [{"id": row["@id"].split("/")[-1],
                 #"name": row[OFFICIAL_NAME][0]['@value'],
                 "parent_id": geocode}
                for row in r.json()]
    return children


def get_children(base, geocodes, max_attempts=5):
    """Wrapper around :obj:`_get_children` in order to search for any children
    of a list of geography codes.

    Args:
        base (str): GSS 3-digit base code of target (child) geographies.
        geocodes (list): GSS 9-digit codes of test (parent) geographies.
        max_attempts (int): Number of :obj:`geocodes` to try before giving up.
    Returns:
        List of dict of children.
    """
    output = []
    found = False
    for icode, code in enumerate(geocodes):
        if icode >= max_attempts and not found:
            break
        children = _get_children(base, code)
        n = len(children)
        if n > 0:
            output += children
            found = True
    return output


def get_gss_codes(test=False):
    """Get all UK geography codes.

    Returns:
        List of UK geography codes.
    """
    CONFIG = find_filepath_from_pathstub("fetch_geography_codes.sparql")
    n = 1 if test else None
    with open(CONFIG) as f:
        query = f.read().replace("\n", " ")
    data = sparql_query(ENDPOINT, query, batch_limit=n)
    return [row["area_code"] for batch in data for row in batch]


if __name__ == "__main__":
    # Get all UK geographies, and group by country and base
    gss_codes = get_gss_codes()
    country_codes = defaultdict(lambda: defaultdict(list))
    for code in gss_codes:
        country = code[0]
        base = code[0:3]
        country_codes[country][base].append(code)

    # Iterate through country and base
    output = []
    for country, base_codes in country_codes.items():
        # Try to find children for each base...
        for base in base_codes.keys():
            for base_, codes in base_codes.items():
                # ...except for the base of the parent
                if base == base_:
                    continue                
                output += get_children(base, codes)
            # Testing
            if len(output) > 0:
                break
        # Testing
        if len(output) > 0:
            break

    # Testing
    for row in output:
        print(row)
