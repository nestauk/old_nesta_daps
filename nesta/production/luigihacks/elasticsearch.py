from elasticsearch import Elasticsearch
import pandas as pd

COUNTRY_LOOKUP="https://s3.eu-west-2.amazonaws.com/nesta-open-data/country_lookup/Countries-List.csv"

def _null_empty_str(body):
    _body = []
    for row in _body:
        _row = row.copy()
        for k, v in row.items():
            if v == '':
                _row[k] = None
        _body.append(_row)
    return _body

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


def _country_detection(body):
    lookup = _country_lookup()
    _body = []
    for row in _body:
        _row = row.copy()
        for k, v in row.items():
            if type(v) is not str:
                continue
            for country in lookup:
                if country in v:
                    _row['terms_of_countryTags'] = lookup[country]
        _body.append(_row)
    return _body


class ElasticsearchPlus(Elasticsearch):
    def __init__(self, 
                 null_empty_str=True,
                 country_detection=False,                 
                 *args, **kwargs):
        self.functions = []
        if null_empty_str:
            self.functions.append(_null_empty_str)
        if country_detection:
            self.functions.append(_country_detection)        
        super().__init__(*args, **kwargs)

    def index(self, **kwargs):
        try:
            _body = kwargs.pop("body")
        except KeyError:
            raise ValueError("Keyword argument 'body' was not provided.")

        for function in self.functions:
            _body = function(_body)
        super().index(body=_body, *args, **kwargs)
