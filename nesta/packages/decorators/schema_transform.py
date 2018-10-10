'''
schema_transform
================

Apply a field name transformation to a data output from the wrapped function,
such that specified field names are transformed and unspecified fields are dropped.
A valid file would be formatted as shown:

[{"tier_0": "bad_col", "tier_1": "good_col"},
{"tier_0": "another_bad_col", "tier_1": "another_good_col"},
...]

where :code:`tier_0` and :code:`tier_1` correspond to :code:`from_key` and :code:`to_key`
in the below documentation.    
'''

import pandas
import json

def load_transformer(filename, from_key, to_key):
    with open(filename) as f:
        _data = json.load(filename)
    transformer = {row[from_key]:row[to_key] for row in _data}
    return transformer


def schema_transform(filename, from_key, to_key):
    '''
    Args:
        filename (str): A record-oriented JSON file path mapping field names
                        denoted by from :code:`from_key` and :code:`to_key`.
        from_key (str): The key in file indicated by :code:`filename` which indicates
                        the field name to transform.
        to_key (str): The key in file indicated by :code:`filename` which what
                      the field name indicated by :code:`from_key` will be transformed to.

    Returns:
        Data in the format it was originally passed to the wrapper in, with 
        specified field names transformed and unspecified fields dropped.
    '''

    transformer = load_transformer(filename, from_key, to_key)
    def wrapper(func):
        def transformed(*args, **kwargs):
            data = func(*args,**kwargs)
            # Accept DataFrames...
            if type(data) == pandas.DataFrame:
                drop_cols = [c for c in data.columns 
                             if c not in transformer]
                data.drop(drop_cols, axis=1, inplace=True)
                data.rename(columns=transformer, inplace=True)
            # ... OR list of dicts
            elif type(data) == list and all(type(row) == dict for row in data):
                data = [{transformer[k]:v for k, v in row.items()
                         if k in transformer} for row in data]
            # Otherwise throw an error
            else:
                raise ValueError("Schema transform expects EITHER a pandas.DataFrame "
                                 "OR a list of dict from the wrapped function.")
            return data
        return transformed
    return wrapper
