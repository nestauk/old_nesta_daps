from collections import Counter
from collections import defaultdict
from elasticsearch import Elasticsearch
from functools import reduce
import numpy as np
import pandas as pd
import re
import string
from copy import deepcopy

from nesta.packages.decorators.schema_transform import schema_transformer

COUNTRY_LOOKUP=("https://s3.eu-west-2.amazonaws.com"
                "/nesta-open-data/country_lookup/Countries-List.csv")
COUNTRY_TAG="terms_of_countryTags"
PUNCTUATION = re.compile(r'[a-zA-Z\d\s:]').sub('', string.printable)


def _null_empty_str(row):
    """Nullify values if they are empty strings.

    Args:
        row (dict): Row of data to evaluate.
    Returns:
        _row (dict): Modified row.
    """
    _row = deepcopy(row)
    for k, v in row.items():
        if v == '':
            _row[k] = None
    return _row

def _coordinates_as_floats(row):
    """Ensure coordinate data are always floats.

    Args:
        row (dict): Row of data to evaluate.
    Returns:
        _row (dict): Modified row.
    """
    _row = deepcopy(row)
    for k, v in row.items():
        if not k.startswith("coordinate_"):
            continue
        if v is None:
            continue
        _row[k]['latitude'] = float(v['latitude'])
        _row[k]['longitude'] = float(v['longitude'])
    return _row

def _country_lookup():
    """Extract country/nationality --> iso2 code lookup
    from a public json file.

    Returns:
        lookup (dict): country/nationality --> iso2 code lookup.
    """
    df = pd.read_csv(COUNTRY_LOOKUP, encoding='latin')
    lookup = defaultdict(list)
    for _, row in df.iterrows():
        iso2 = row.pop("ISO 3166 Code")
        for k, v in row.items():
            if pd.isnull(v):
                continue
            lookup[v].append(iso2)
    return lookup

def _country_detection(row, lookup):
    """Append a list of countries detected from keywords
    discovered in all text fields. The new field name
    is titled according to the global variable COUNTRY_TAG.

    Args:
        row (dict): Row of data to evaluate.
    Returns:
        _row (dict): Modified row.
    """
    _row = deepcopy(row)
    _row[COUNTRY_TAG] = []
    for k, v in row.items():
        if type(v) is not str:
            continue
        for country in lookup:
            if country in v:
                _row[COUNTRY_TAG] += lookup[country]
    tags = _row[COUNTRY_TAG]
    _row[COUNTRY_TAG] = None if len(tags) == 0 else list(set(tags))
    return _row


def _guess_delimiter(item, threshold=0.25):
    """Guess the delimiter in a splittable string.
    Note, the delimiter is assumed to be non-whitespace
    non-alphanumeric.

    Args:
        item (str): A string that we want to split up.
        threshold (float): If the mean fractional size of the split items
                           are greater than this threshold, it is assumed
                           that no delimiter exists.
    Returns:
        p (str): A delimiter character.
    """
    scores = {}
    for p in PUNCTUATION:
        split = item.split(p)
        mean_size = np.mean(list(map(len, split)))
        scores[p] = mean_size/len(item)
    p, score = Counter(scores).most_common()[-1]
    if score < threshold:
        return p


def _listify_terms(row):
    """Split any 'terms' fields by a guessed delimiter if the
    field is a string.

    Args:
        row (dict): Row of data to evaluate.
    Returns:
        _row (dict): Modified row.
    """
    _row = deepcopy(row)
    for k, v in row.items():
        if not k.startswith("terms_"):
            continue
        if v is None:
            continue
        _type = type(v)
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
    """Convert any values to null if the type of
    the value is listed in the field_null_mapping
    for that field. Note that a special keyword '<NEGATIVE>'
    exists to signify that negative numbers should be nulled.
    For example a field_null_mapping of:

    {
        "field_1": ["", 123, "a", "<NEGATIVE>"],
        "field_2": [""]
    }

    would lead to the data:

    [{"field_1": 123, "field_2": "a"},
     {"field_1": "", "field_2": ""},
     {"field_1": -23, "field_2": 123}]

    being converted to:

    [{"field_1": None, "field_2": "a"},
     {"field_1": None, "field_2": None},
     {"field_1": None, "field_2": 123}]

    Args:
        row (dict): Row of data to evaluate.
        field_null_mapping (dict): Mapping of field names to values to be interpreted as null.
    Returns:
        _row (dict): Modified row.
    """
    _row = deepcopy(row)
    for field_name, nullable_values in field_null_mapping.items():
        if type(nullable_values) is not list:
            raise ValueError("Nullable values in field_null_mapping should be a list "
                             f"but {type(nullable_values)} [{nullable_values}] found.")
        if field_name not in _row:
            continue
        value = _row[field_name]
        null_negative = (("<NEGATIVE>" in nullable_values) and
                         (type(value) in (float, int)) and
                         (value < 0))
        # This could apply to coordinates
        if type(value) is dict:
            for k, v in value.items():
                if {k: v} in nullable_values:
                    _row[field_name] = None
                    break
        # For non-iterables
        elif value in nullable_values or null_negative:
            _row[field_name] = None
    return _row


class ElasticsearchPlus(Elasticsearch):
    """Wrapper around the Elasticsearch API, which applies
    transformations (including schema mapping) to input data
    before indexing.

    Args:
        schema_transformer_kwargs (dict): Schema transformation keyword arguments.
        field_null_mapping: A mapping of fields to values to be converted to None.
        null_empty_str (bool): Convert empty strings to None?
        coordinates_as_floats (bool): Convert all coordinate fields to floats?
        country_detection (bool): Append new field listing country name mentions?
        listify_terms (bool): Attempt to convert all 'terms' fields to lists?
        {args, kwargs}: (kw)args for the core :obj:`Elasticsearch` API.
    """
    def __init__(self,
                 strans_kwargs={},
                 field_null_mapping={},
                 null_empty_str=True,
                 coordinates_as_floats=True,
                 country_detection=True,
                 listify_terms=True,
                 *args, **kwargs):
        # Apply the schema mapping
        self.transforms = [lambda row: schema_transformer(row, **strans_kwargs)]

        # Convert values to null as required
        if null_empty_str:
            self.transforms.append(_null_empty_str)

        # Convert other values to null as specified
        if len(field_null_mapping) > 0:
            self.transforms.append(lambda row: _null_mapping(row,
                                                             field_null_mapping))

        # Convert coordinates to floats
        if coordinates_as_floats:
            self.transforms.append(_coordinates_as_floats)

        # Detect countries in text fields
        if country_detection:
            _lookup = _country_lookup()
            self.transforms.append(lambda row: _country_detection(row, _lookup))

        # Convert items which SHOULD be lists to lists
        if listify_terms:
            self.transforms.append(_listify_terms)
        super().__init__(*args, **kwargs)

    def chain_transforms(self, row):
        """Apply all transforms sequentially to a given row of data.

        Args:
            row (dict): Row of data to evaluate.
        Returns:
            _row (dict): Modified row.
        """
        return reduce(lambda _row, f: f(_row), self.transforms, row)

    def index(self, **kwargs):
        """Same as the core :obj:`Elasticsearch` API, except applies the
        transformation chain before indexing. Note: only keyword arguments
        are accepted.
        """
        try:
            _body = kwargs.pop("body")
        except KeyError:
            raise ValueError("Keyword argument 'body' was not provided.")
        _body = map(lambda row: self.chain_transforms(row), _body)
        super().index(body=list(_body), **kwargs)
