"""
NOMIS
=====

Collect official data from the NOMIS API, using configuration files.
"""

import requests
from collections import Counter
from nesta.production.luigihacks.misctools import get_config
from io import StringIO
import pandas as pd
from collections import defaultdict
import re
import logging
import datetime

NOMIS = "http://www.nomisweb.co.uk/api/v01/dataset/{}"
NOMIS_DEF = NOMIS.format("{}def.sdmx.json")
REGEX = re.compile("(\w+)_code$")

def get_base(x):
    """Get the NOMIS column name base entity, for columns named '{something}_code'.
    
    Args:
        x (str): The NOMIS column name, of the form {something}_code
    Returns:
        base (str): The NOMIS column name base entity
    """
    return REGEX.findall(x)[0]


def get_code_pairs(columns):
    """Pair NOMIS key-value column names.

    Args:
        columns (list): A list of NOMIS column names.
    Returns:
        cols (dict): Mapping of key-value column names.
    """
    cols = {c: f"{get_base(c)}_name"
            for c in columns if c.endswith("_code")
            if f"{get_base(c)}_name" in columns}
    return cols


def reformat_nomis_columns(data):
    """Reformat columns from default NOMIS data.

    Args:
        df (:obj:`pd.DataFrame`): Dataframe containing NOMIS data.
    Returns:
        tables (:obj:`list` of :obj:`dict`): Reformatted rows of data.
    """
    tables = defaultdict(list)
    # Generate the NOMIS key-value column pairs, and recursively
    # replace them
    for name, df in data.items():
        _df = df.copy()  # Don't mess with the original data
        pairs = get_code_pairs(_df.columns)
        for _code, _name in pairs.items():
            base = f"{get_base(_code)}_lookup"
            df_codes = _df[[_name,_code]].copy()
            df_codes.columns = ["name","code"]
            tables[base] += df_codes.to_dict(orient="records")
            _df.drop(_name, axis=1, inplace=True)
        tables[name] = _df.to_dict(orient="records")
        for row in tables[name]:
            row['date'] = row['date'].to_pydatetime()
    # Append this pair of values
    return tables

def batch_request(config, dataset_id, geographies, date_format, 
                  record_offset=0, max_api_calls=10):
    """Fetch a NOMIS dataset from the API, in batches, 
    based on a configuration object.
    
    Args:                                                               
        config (dict): Configuration object, from which a get
                       request is formed.
        dataset_id (str): NOMIS dataset ID
        geographies (list): Return object from :obj:`discovery_iter`.
        date_format (str): Formatting string for dates in the dataset
        record_offset (int): Record to start from
        max_api_calls (int): Number of requests allowed
    Returns:                                                            
        dfs (:obj:`list` of :obj:`pd.DataFrame`): Batch return results.      
    """
    config["geography"] = ",".join(str(row["nomis_id"]) 
                                   for row in geographies)
    config["RecordOffset"] = record_offset
    date_parser = lambda x: pd.datetime.strptime(x, date_format)

    # Build a list of dfs in chunks from the NOMIS API
    dfs = []
    offset = 25000
    icalls = 0
    done = False
    while (not done) and icalls < max_api_calls:
        #logging.debug(f"\t\t {offset}")
        # Build the request payload
        params = "&".join(f"{k}={v}" for k,v in config.items())
        # Hit the API
        r = requests.get(NOMIS.format(f"{dataset_id}.data.csv"), params=params)
        # Read the data
        with StringIO(r.text) as sio:
            _df = pd.read_csv(sio, parse_dates=["DATE"], date_parser=date_parser)
            done = len(_df) < offset
        # Increment the offset
        config["RecordOffset"] += offset
        # Ignore empty fields
        dfs.append(_df.loc[_df.OBS_VALUE > 0])
        icalls += 1
    
    # Combine and return
    df = pd.concat(dfs)
    df.columns = [c.lower() for c in df.columns]
    return df, done, config["RecordOffset"]



def process_config(conf_prefix, test=False):
    """Fetch a NOMIS dataset from the API based on a configuration file.

    Args:
        conf_prefix (str): Configuration file name prefix, such that
                           a configuration file exists in the global
                           config file directory (see :obj:`get_config`)
                           of the form
                           'official_data/{conf_prefix}.config'
    Returns:
        df (:obj:`pd.DataFrame`): Dataframe containing NOMIS data.
    """
    # Get the configuration
    config = get_config(f"official_data/{conf_prefix}.config", "nomis")
    #logging.debug("\tGot config")
    dataset_id = config.pop("dataset")
    date_format = config.pop("date_format")
    # Iterate over NOMIS geography codes for this dataset
    geogs_list = []
    for geo_type in config.pop("geography_type").split(","):
        #logging.debug(f"\t{geo_type}")
        geographies = find_geographies(geo_type, dataset_id)
        if test:
            geographies = [geographies[0]]
        #logging.debug(f"\tGot {len(geographies)} geographies")
        geogs_list.append(geographies)
    return config, geogs_list, dataset_id, date_format


def jaccard_repeats(a, b):
    """Jaccard similarity measure between input iterables,
    allowing repeated elements"""
    _a = Counter(a)
    _b = Counter(b)
    c = (_a - _b) + (_b - _a)
    n = sum(c.values())
    return 1 - n/(len(a) + len(b) - n)


def discovery_iter(dataset_id):
    """Iterate through the NOMIS discovery API
    
    Args:
        dataset_id (str): ID of the dataset to inspect, which fits into the API request
                          as http://www.nomisweb.co.uk/api/v01/dataset/{dataset_id}def.sdmx.json.
    Yields:
        _row_data (dict): A flattened row of discovery data.
    """
    # Hit the discovery API
    r = requests.get(NOMIS_DEF.format(dataset_id))
    r.raise_for_status()
    # Dead end if no codelists are present
    data = r.json()
    codelists = data['structure']['codelists']
    if codelists is None:
        return
    # Iterate and flatten the codelist
    for row in codelists['codelist']:
        for _row in row['code']:
            # Extract annotation details
            _row_data = {item['annotationtitle']: item['annotationtext']
                         for item in _row['annotations']['annotation']}
            # ... and name and ID
            _row_data['name'] = _row['description']['value']
            _row_data['nomis_id'] = _row['value']
            yield _row_data


def find_geographies(geography_name, dataset_id = "NM_1_1"):
    """Find all geography codes based on the name of the geography type.

    Args:
        geography_name (str): NOMIS geography type name, such as "regions" or
                              "local authorities: district / unitary (prior to April 2015)".
    Returns:
        List of geography codes, if found.
    """
    # Iterate two layers deep to find all the geography type names
    matches = {}
    for row in discovery_iter(f"{dataset_id}/geography."):
        for sub_row in discovery_iter(f"{dataset_id}/geography/{row['nomis_id']}."):
            # Don't repeat any previously found geography types
            test_name = sub_row['TypeName']
            if test_name in matches:
                continue
            # Calculate the Jaccard similarity between this geography type, and
            # the desired geography name. This will only be used to help the
            # user find a close match in the case that no geography codes
            # are found.
            matches[test_name] = jaccard_repeats(geography_name, test_name)
            # Exclude inexact matches
            if test_name != geography_name:
                continue
            # Return if a match is found
            return list(discovery_iter(f"{dataset_id}/geography/{sub_row['nomis_id']}."))
    # No match has been found, so give a recommendation
    best, _ = Counter(matches).most_common(1)[0]
    raise ValueError(f"No result found, did you mean '{best}'?")


if __name__ == "__main__":
    configs = ["businesscount", "employment", #"population_census",
               "population_estimate", "workforce_jobs"]


    for config_name in configs:
        config, geogs_list, dataset_id, date_format = process_config(config_name, 
                                                                     test=False)
        for igeo, geographies in enumerate(geogs_list):
            print(igeo)
            done = False
            record_offset = 0
            while not done:
                print("\t", record_offset)
                df, done, record_offset = batch_request(config, dataset_id, geographies,
                                                        date_format, max_api_calls=2, 
                                                        record_offset=record_offset)
                data = {config_name: df}
                tables = reformat_nomis_columns(data)
                for name, table in tables.items():
                    print("\t\t", name, len(table))


    # nomis_data = {}
    # for table_name in configs:
    #     nomis_data[table_name] = get_nomis_data(table_name)

    # tables = reformat_nomis_columns(nomis_data)
    # for table_name, data in tables.items():
    #     print(table_name, len(data))
