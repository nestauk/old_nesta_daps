from googletrans import Translator
from html.parser import HTMLParser
from collections import Counter
from collections import defaultdict
from collections import OrderedDict
from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
from retrying import retry
from functools import reduce
import numpy as np
import pandas as pd
import re
import string
from copy import deepcopy
import boto3
from requests_aws4auth import AWS4Auth
import time
import os

from nesta.packages.nlp_utils.ngrammer import Ngrammer
from nesta.packages.decorators.schema_transform import schema_transformer
from nesta.packages.decorators.ratelimit import ratelimit

COUNTRY_LOOKUP = ("https://s3.eu-west-2.amazonaws.com"
                  "/nesta-open-data/country_lookup/Countries-List.csv")
COUNTRY_TAG = "terms_of_countryTags"
TRANS_TAG = "booleanFlag_autotranslated_entity"
LANGS_TAG = "terms_iso2lang_entity"
PUNCTUATION = re.compile(r'[a-zA-Z\d\s:]').sub('', string.printable)

def sentence_chunks(text, chunksize=2000, delim='. '):
    """Split a string into chunks, but breaking only on
    the specified delimiter.

    Args:
        text (str): A string to split into chunks
        chunksize (int): Minimum chunk size to yield
        delim (str): Delimiter to split chunks
    Yields:
        chunk (str): The smallest possible chunks (minimum size
                     :pyobject:`chunksize`) of text.
    """
    chunks = []
    for _text in text.split(delim):
        chunks.append(_text)
        if sum(len(c) for c in chunks) > chunksize:
            yield delim.join(chunks)
            chunks = []
    if len(chunks) > 0:
        yield delim.join(chunks)


class MLStripper(HTMLParser):
    """Taken from https://stackoverflow.com/questions/753052/strip-html-from-strings-in-python. Tested in _sanitize_html."""
    def __init__(self):
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.fed = []
        super().__init__()
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    """Taken from https://stackoverflow.com/questions/753052/strip-html-from-strings-in-python. Tested in _sanitize_html"""
    s = MLStripper()
    s.feed(html)
    return s.get_data()


@ratelimit(max_per_second=2.5)
@retry(wait_random_min=20, wait_random_max=30,
       stop_max_attempt_number=10)
def translate(text, translator, chunksize=2000):
    """Translate texts of any length via the Google Translate API.

    Args:
        text (str): text to translate to English.
        translator: A Translator instance.
        chunksize (int): Ideal chunk size of text. Note, text is
                         chunked in sentences defined by '. '
    Returns:
        {text, langs} ({str, set}): Translated text and set of
                                    detected languages.
    """
    chunks = [strip_tags(t) for t in sentence_chunks(text, chunksize=chunksize)]
    texts, langs = [], set()
    for t in translator.translate(chunks, dest='en'):
        texts.append(t.text.capitalize())  # GT uncapitalizes chunks
        langs.add(t.src)
    return '. '.join(texts), langs


def _auto_translate(row, translator=None, min_len=150, chunksize=2000, service_urls=[]):
    """Translate any text fields longer than min_len characters
    into English.

    Args:
        row (dict): Row of data to evaluate.
    Returns:
        _row (dict): Modified row.
    """
    if translator is None:
        translator = Translator(service_urls=service_urls)
    _row = deepcopy(row)
    _row[TRANS_TAG] = False
    _row[LANGS_TAG] = set()
    for k, v in row.items():
        if type(v) is not str:
            continue
        if len(v) <= min_len:
            continue
        result, langs = translate(v, translator, chunksize)
        _row[LANGS_TAG] = _row[LANGS_TAG].union(langs)
        if langs == {'en'}:
            continue
        _row[k] = result
        _row[TRANS_TAG] = True
    _row[LANGS_TAG] = list(_row[LANGS_TAG])
    return _row


def _ngram_and_tokenize(row, ngrammer, ngram_fields):
    tokens = []
    _row = deepcopy(row)
    for field in ngram_fields:
        text = _row[field]
        if type(text) is not str:
            continue
        processed_tokens = ngrammer.process_document(text)
        tokens += [t.replace('_', ' ')
                   for tokens in processed_tokens
                   for t in tokens]    
    _row['terms_tokens_entity'] = tokens
    return _row
    

def _sanitize_html(row):
    """Strips out any html encoding. Note: nothing clever is done
    such as replacing breaks with newlines.

    Args:
        row (dict): Row of data to evaluate.
    Returns:
        _row (dict): Modified row.
    """
    _row = deepcopy(row)
    for k, v in row.items():
        if type(v) is not str:
            continue
        _row[k] = strip_tags(v)
    return _row

def _clean_bad_unicode_conversion(row):
    """Removes sequences of ??? from strings, which normally
    occur due to bad unicode conversion. Note this is a hack:
    the real solution is to deal with unicode gracefully, where
    the option is available.

    Args:
        row (dict): Row of data to evaluate.
    Returns:
        _row (dict): Modified row.
    """
    _row = deepcopy(row)
    for k, v in row.items():
        if type(v) is not str:
            continue
        elif "??" not in v:
            continue
        while "???" in v:
            v = v.replace("???","")
        while "??" in v:
            v = v.replace("??","")
        _row[k] = v
    return _row

def _nullify_pairs(row, null_pairs={}):
    """Nullify any value if it's 'parent' is also null.
    For example for null_pairs={'parent': 'child'}
    the following will occur:

    {'parent': None, 'child': 5} will become {'parent': None, 'child': None}

    however

    {'parent': 5, 'child': None} will remain unchanged.

    Args:
        row (dict): Row of data to evaluate.
        null_pairs (dict): Null mapping, as described above.
    Returns:
        _row (dict): Modified row.
    """
    _row = deepcopy(row)
    for parent, child in null_pairs.items():
        if _row[parent] is None:
            _row[child] = None
    return _row

def _remove_padding(row):
    """Remove padding from text or list text

    Args:
        row (dict): Row of data to evaluate.
    Returns:
        _row (dict): Modified row.
    """
    _row = deepcopy(row)
    for k, v in row.items():
        if type(v) is str:
            _row[k] = v.strip()
        elif type(v) is list:
            _row[k] = [item.strip() if type(item) is str else item
                       for item in v]
    return _row

def _caps_to_camel_case_by_value(v):
    if type(v) is not str:
        return v
    if len(v) < 4:
        return v
    if v != v.upper():
        return v
    return v.lower().title()

def _caps_to_camel_case(row):
    """Convert CAPITAL TERMS to Camel Case

    Args:
        row (dict): Row of data to evaluate.
    Returns:
        _row (dict): Modified row.
    """
    _row = deepcopy(row)
    for k, v in row.items():
        if type(v) is str:
            _row[k] = _caps_to_camel_case_by_value(v)
        elif type(v) is list:
            _row[k] = [_caps_to_camel_case_by_value(item) for item in v]
    return _row


def _clean_up_lists(row, do_sort=True):
    """Deduplicate, remove None and nullify empties in any list fields.

    Args:
        row (dict): Row of data to evaluate.
    Returns:
        _row (dict): Modified row.
    """
    _row = deepcopy(row)
    for k, v in row.items():
        if type(v) is not list:
            continue
        if len(v) > 0 and type(v[0]) is dict:
            continue
        # Remove empty strings
        to_remove = [item for item in v
                     if type(item) is str and item.strip() == ""]
        for item in to_remove:
            v.remove(item)
        v = list(set(v))  # deduplicate
        # Remove Nones
        if None in v:
            v.remove(None)
        # Nullify empty lists
        if len(v) == 0:
            v = None
        elif do_sort:
            v = sorted(v)
        _row[k] = v
    return _row

def _add_entity_type(row, entity_type):
    _row = deepcopy(row)
    _row['type_of_entity'] = entity_type
    return _row

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
        if v['lat'] is None or v['lon'] is None:
            _row[k] = None
            continue
        _row[k]['lat'] = float(v['lat'])
        _row[k]['lon'] = float(v['lon'])
    return _row

def _country_lookup():
    """Extract country/nationality --> iso2 code lookup
    from a public json file.

    Returns:
        lookup (dict): country/nationality --> iso2 code lookup.
    """
    df = pd.read_csv(COUNTRY_LOOKUP, encoding='latin', na_filter = False)
    lookup = defaultdict(list)
    for _, row in df.iterrows():
        iso2 = row.pop("ISO 3166 Code")
        for k, v in row.items():
            if pd.isnull(v) or len(v) == 0:
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
            if country not in v:
                continue
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

def _listify_terms(row, delimiters=None):
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
        if delimiters is None:
            delimiter = _guess_delimiter(v)
            if delimiter is not None:
                _row[k] = v.split(delimiter)
            else:
                _row[k] = [v]
        else:
            for d in delimiters:
                v = v.replace(d, "SPLITME")
            _row[k] = v.split("SPLITME")
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
        all_fields = (field_name == "<ALL_FIELDS>")
        if not (all_fields or field_name in _row):
            continue
        # Special case: apply these null mappings to any field
        if all_fields:
            # Iterate over all fields
            for k, v in row.items():
                # If the value is nullable...
                if v in nullable_values:
                    _row[k] = None
                # ...or if the value is a list...
                if type(v) is not list:
                    continue
                # ... then remove any nullable value
                for item in v:
                    while (item in v) and (item in nullable_values):
                        v.remove(item)
                _row[k] = v
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
        aws_auth_region (str): AWS region to be for authentication.
                               If not None, use HTTPS and AWS
                               authentication from boto3 credentials.
        no_commit (bool): Call the super index method? Useful for
                          dry-runs of the transformation chain.
        schema_transformer_kwargs (dict): Schema transformation keyword arguments.
        field_null_mapping: A mapping of fields to values to be converted to None.
        null_empty_str (bool): Convert empty strings to None?
        coordinates_as_floats (bool): Convert all coordinate fields to floats?
        country_detection (bool): Append new field listing country name mentions?
        listify_terms (bool): Attempt to convert all 'terms' fields to lists?
        terms_delimiters (tuple): Convert all 'terms' fields to list if these delimiters are specified.
        caps_to_camel_case (bool): Convert all upper case text fields (longer than 3 chars)
                                   to camel case?
        remove_padding (bool): Remove all whitespace padding?
        auto_translate (bool): Convert large text fields to English?
        do_sort (bool): Sort all lists?
        {args, kwargs}: (kw)args for the core :obj:`Elasticsearch` API.
    """
    def __init__(self, entity_type,
                 aws_auth_region,
                 no_commit=False,
                 strans_kwargs=None,
                 field_null_mapping={},
                 null_empty_str=True,
                 coordinates_as_floats=True,
                 country_detection=False,
                 listify_terms=True,
                 terms_delimiters=None,
                 caps_to_camel_case=False,
                 null_pairs={},
                 auto_translate=False,
                 do_sort=True,
                 auto_translate_kwargs={},
                 ngram_fields=[],
                 *args, **kwargs):

        self.no_commit = no_commit
        # If aws auth is required, fill up the kwargs with more
        # arguments to pass to the core API.
        credentials = boto3.Session().get_credentials()
        http_auth = AWS4Auth(credentials.access_key,
                             credentials.secret_key,
                             aws_auth_region, 'es')
        kwargs["http_auth"] = http_auth
        kwargs["use_ssl"] = True
        kwargs["verify_certs"] = True
        kwargs["connection_class"] = RequestsHttpConnection

        # Apply the schema mapping
        self.transforms = []
        if strans_kwargs is not None:
            self.transforms.append(lambda row: schema_transformer(row,
                                                                  **strans_kwargs))
        self.transforms.append(lambda row: _add_entity_type(row,
                                                            entity_type))

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
            self.transforms.append(lambda row: _listify_terms(row, terms_delimiters))

        # Convert upper case text to camel case
        if caps_to_camel_case:
            self.transforms.append(_caps_to_camel_case)

        # Translate any text to english
        if auto_translate:
            # URLs to load balance Google Translate
            urls = list(f"translate.google.{ext}"
                        for ext in ('com', 'co.uk', 'co.kr', 'at',
                                    'ru', 'fr', 'de', 'ch', 'es'))
            self.transforms.append(lambda row: _auto_translate(row, translator=None,
                                                               service_urls=urls,
                                                               **auto_translate_kwargs))

        # Extract any ngrams and split into tokens
        if len(ngram_fields) > 0:
            # Setup ngrammer
            if 'MYSQLDBCONF' not in os.environ:                
                os.environ['MYSQLDBCONF'] = 'mysqldb.config'
            ngrammer = Ngrammer(database="production") 
            self.transforms.append(lambda row: _ngram_and_tokenize(row, ngrammer,
                                                                   ngram_fields))

        # Clean up lists (dedup, remove None, empty lists are None)
        self.transforms.append(_sanitize_html)
        self.transforms.append(_clean_bad_unicode_conversion)
        self.transforms.append(lambda row: _clean_up_lists(row, do_sort))
        self.transforms.append(_remove_padding)
        self.transforms.append(lambda row: _nullify_pairs(row, null_pairs))
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
        are accepted for core :obj:`Elasticsearch` API.

        Args:
            kwargs: All kwargs to pass to the core Elasticsearch API.
        Returns:
            body (dict): The transformed body, as passed to Elasticsearch.
        """
        try:
            _body = kwargs.pop("body")
        except KeyError:
            raise ValueError("Keyword argument 'body' was not provided.")

        body = dict(self.chain_transforms(_body))
        # Sort the body
        body = OrderedDict(sorted(body.items()))
        if not self.no_commit:
            super().index(body=body, **kwargs)
        return body

    def near_duplicates(self, index, doc_id,
                        fields,
                        doc_type,
                        threshold=0.98,
                        min_term_freq=1,
                        max_query_terms=25):
        """Yield near duplicate documents, compared to the input
        document id.

        Args:
            index (str): Index in which to scan for documents.
            doc_id (str): Document id for which to find duplicates.
            fields (list): List of fields to query for duplicates.
            doc_type (str): Document type to supply to ES.
            threshold (float): Minimum document similarity (0 to 1).
            min_term_freq (int): See Elasticsearch MoreLikeThis docs.
            max_query_terms (int): See Elasticsearch MoreLikeThis docs.
        """
        # Make the query
        mlt_query = {"fields": fields,
                     "min_term_freq": min_term_freq,
                     "max_query_terms": max_query_terms,
                     "include": True,
                     "like": [{'_index': index, '_id': doc_id}]}
        body = {"query": {"more_like_this": mlt_query}}
        results = self.search(index=index, body=body)

        # Mock a result if there are no results
        if results['hits']['total'] == 0:
            _doc = self.get(index=index,
                            doc_type=doc_type,
                            id=doc_id)
            _doc['_score'] = 1
            results['hits']['max_score'] = 1
            results['hits']['hits'] = [_doc]

        # Yield duplicates
        max_score = results['hits']['max_score']
        hits = []
        for hit in results['hits']['hits']:
            score = hit['_score']
            # Break when the score is too different
            # (note: the results are sorted by score)
            if score/max_score < threshold:
                break
            yield hit
