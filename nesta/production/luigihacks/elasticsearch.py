from collections import Counter
from elasticsearch import Elasticsearch
from functools import reduce
import numpy as np
import pandas as pd
import re
import string

from nesta.packages.decorators.schema_transform import schema_transformer

COUNTRY_LOOKUP="https://s3.eu-west-2.amazonaws.com/nesta-open-data/country_lookup/Countries-List.csv"
PUNCTUATION = re.compile('[a-zA-Z\d\s:]').sub('', string.printable)


def _null_empty_str(row):
    _row = row.copy()
    for k, v in row.items():
        if v == '':
            _row[k] = None
    return _row


def _coordinates_as_floats(row):
    _row = row.copy():
    for k, v in row.items():
        if not k.startswith("coordinate_"):
            continue
        if v is None:
            continue
        _row[k]['latitude'] = float(v['latitude'])
        _row[k]['longitude'] = float(v['longitude'])
    return _row


def _country_lookup():
    df = pd.read_csv(COUNTRY_LOOKUP, encoding='latin')
    lookup = {}
    for _, row in df.iterrows():
        iso2 = row.pop("ISO 3166 Code")
        for k, v in row.items():
            if pd.isnull(v):
                continue
            if v in lookup:
                raise ValueError(f"Duplicate value: {v}")
            lookup[v] = iso2
    return lookup


def _country_detection(row, lookup):
    _row = row.copy()
    for k, v in row.items():
        if type(v) is not str:
            continue
        for country in lookup:
            if country in v:
                _row['terms_of_countryTags'] = lookup[country]
    return _row


def _guess_delimiter(item, threshold=0.1):
    scores = {}
    for p in PUNCTUATION:
        split = item.split(p)
        mean_size = np.mean(list(map(len, split)))
        scores[p] = mean_size/len(item)
    p, score = Counter(scores).most_common()[-1]
    if score < threshold:
        return p


def _listify_terms(row):
    """Terms must be a list, so either split by most common delimeter
    (“;” for RWJF, must have a frequency of n%) or convert to a list
    """
    _row = row.copy()
    for k, v in row.items():
        if not k.startswith("terms_"):
            continue
        if v is None:
            continue
        _type = type(k)
        if _type is list:
            continue
        elif _type is not str:
            raise TypeError(f"Type for '{k}' is '{_type}' but expected 'str' or 'list'.")
        # Now determine the delimiter
        delimiter = _guess_delimiter(v)
        if delimiter is not None:
            _row[k] = v.split(delimiter)
        else:
            _row[k] = [v]
    return _row

def _null_mapping(row, field_null_mapping):
    _row = row.copy()
    for field_name, nullable_values in field_null_mapping.items():
        if field_name not in _row:
            continue
        if _row[field_name] in nullable_values:
            _row[field_name] = None
    return _row


class ElasticsearchPlus(Elasticsearch):
    def __init__(self,
                 schema_transformer_args=(),
                 schema_transformer_kwargs={},
                 field_null_mapping={},
                 null_empty_str=True,
                 coordinates_as_floats=True,
                 country_detection=False,
                 listify_terms=True,
                 *args, **kwargs):
        # Apply the schema mapping
        self.functions = [lambda row: schema_transformer(row, *schema_transformer_args,
                                                         **schema_transformer_kwargs)]
        # Convert values to null as required
        if null_empty_str:
            self.functions.append(_null_empty_str)
        if len(field_null_mapping) > 0:
            self.functions.append(lambda row: _null_mapping(row, field_null_mapping))
        if coordinates_as_floats:
            self.functions.append(_coordinates_as_floats)
        # Detect countries in text fields
        if country_detection:
            _lookup = _country_lookup()
            self.functions.append(lambda row: _country_detection(row, _lookup))
        # Convert items which SHOULD be lists to lists
        if listify_terms:
            self.functions.append(listify_terms)
        super().__init__(*args, **kwargs)

    def chain_functions(self, row):
        return reduce(lambda _row, f: f(_row), self.functions, row)

    def index(self, **kwargs):
        try:
            _body = kwargs.pop("body")
        except KeyError:
            raise ValueError("Keyword argument 'body' was not provided.")
        _body = map(lambda row: chain_functions(row), _body)
        super().index(body=list(_body), *args, **kwargs)
