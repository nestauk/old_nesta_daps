import requests
from retrying import retry
import json
from collections import defaultdict

WORLDBANK_ENDPOINT = "http://api.worldbank.org/v2/{}"
DEAD_RESPONSE = [None, None]


def worldbank_request(suffix, page, per_page=10000, data_key_path=None):
    response = _worldbank_request(suffix=suffix, page=page, per_page=per_page)
    metadata, data = data_from_response(response=response,
                                        data_key_path=data_key_path)
    return metadata, data


def _worldbank_request(suffix, page, per_page):
    r = requests.get(WORLDBANK_ENDPOINT.format(suffix),
                     params=dict(per_page=per_page, format="json", page=page))
    if r.status_code == 400:
        return DEAD_RESPONSE

    r.raise_for_status()
    try:
        response = r.json()
    except json.JSONDecodeError:
        response = DEAD_RESPONSE
    finally:
        return response


def data_from_response(response, data_key_path=None):
    if data_key_path is None:
        metadata, datarows = response
    else:
        metadata = response
        datarows = response.copy()
        for key in data_key_path:
            datarows = datarows[key]
            if key != data_key_path[-1] and type(datarows) is list:
                datarows = datarows[0]
    return metadata, datarows


def worldbank_data(suffix, data_key_path=None):
    metadata, _ = worldbank_request(suffix=suffix, page=1, per_page=1,
                                    data_key_path=data_key_path)
    if metadata is None:
        return

    total = int(metadata["total"])
    n, page = 0, 1
    while n < total:
        _, datarows = worldbank_request(suffix=suffix, page=page,
                                        data_key_path=data_key_path)
        for row in datarows:
            yield row
        page += 1
        n += len(datarows)


def get_worldbank_resource(resource):
    countries = []
    for row in worldbank_data(resource):
        data = {}
        for k, v in row.items():
            if type(v) is dict:
                v = v["value"]
            data[k] = v
        countries.append(data)
    return countries


def get_variables_by_code(codes):
    variables = defaultdict(list)
    sources = get_worldbank_resource("source")
    for source in sources:
        suffix = f"sources/{source['id']}/series/data"
        data = worldbank_data(suffix, data_key_path=["source", "concept",
                                                     "variable"])
        filtered_data = filter(lambda row: (row['id'] in codes), data)
        for row in filtered_data:
            variables[row['id']].append(source['id'])
    return variables


def unpack_quantity(row, concept, value):
    for quantity in row['variable']:
        if quantity['concept'] == concept:
            return quantity[value]


def unpack_data(row):
    country = unpack_quantity(row, 'Country', 'id')
    variable = unpack_quantity(row, 'Series', 'value')
    value = row['value']
    return country, variable, value


def get_country_data(variables):
    country_data = defaultdict(dict)
    for series, sources in variables.items():
        variable_name = None
        done_countries = set()
        for source in sources:
            suffix = (f"sources/{source}/country/all/"
                      f"series/{series}/time/YR2010/data")
            data = worldbank_data(suffix, data_key_path=["source", "data"])
            for country, variable, value in map(unpack_data, data):
                if value is None:
                    continue
                if country in done_countries:
                    continue
                if variable_name is None:
                    variable_name = variable
                done_countries.add(country)
                country_data[country][variable_name] = value
    return country_data


def flatten_country_data(country_data, country_metadata):
    flat_data = [dict(**country_data[metadata['id']], **metadata)
                 for metadata in country_metadata
                 if metadata['id'] in country_data]
    return flat_country_data


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
