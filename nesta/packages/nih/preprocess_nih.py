"""
preprocess_nih
==============

Data cleaning / wrangling before ingestion of raw data,
specifically:

  * Splitting of strings into arrays as indicated by JSON the ORM,
  * CAPS to Camel Case for any string field which isn't VARCHAR(n) < 10
  * Inferring how to correctly deal with mystery question marks
  * Dealing consistently with null values
"""
from sqlalchemy.dialects.mysql import VARCHAR, JSON
from functools import lru_cache
import pandas as pd

GENERIC_PREFIXES = ['proposal narrative', 'project narrative', 
                    'relevance to public health', 'public health relevance', 
                    'narrative', 'relevance', 'statement', 
                    '(relevance to veterans)', 'relevance to the va',
                    'description', '(provided by applicant)', 
                    'project summary', 'abstract', 'overall', 'project',
                    '? ', ' ', '/', 'administrative', '.', 'core', 'title',
                    'summary', '*', 'pilot project', '#']
                    
@lru_cache()
def get_json_cols(orm):
    return {col.name for col in orm.__table__.columns
            if type(col.type) is JSON}


@lru_cache()
def get_long_text_cols(orm, min_length=10):
    return {col.name for col in orm.__table__.columns
            if col.type.python_type is str and
            not (type(col.type) == VARCHAR  and col.type.length < 10)}


def is_nih_null(value, nulls={'', [], 'N/A', 'Not Required', 'None'}):
    return (pd.isnull(value) or
            value in nulls)


@lru_cache()
def expand_prefix_list():
    prefixes = []
    numbers = list(str(i) for i in range(0, 10))
    for prefix in GENERIC_PREFIXES + numbers:
        _prefixes = [prefix]
        _prefixes += [prefix+ext for ext in ['', '/', ':', '-', ' -']]
        _prefixes += [ext+prefix for ext in ['?']] 
        prefixes += _prefixes
    prefixes += [p+' ' for p in prefixes]
    prefixes += [p.upper() for p in prefixes]
    prefixes += [p.title() for p in prefixes]
    return sorted(set(prefixes), key=lambda x: len(x), reverse=True)


def remove_generic_suffixes(text):
    still_replacing = True
    while still_replacing:
        still_replacing = False
        for prefix in expand_prefix_list():
            if not text.startswith(prefix):
                continue
            text = text.replace(prefix, '')
            still_replacing = True
            break
    return text


def remove_large_spaces(text):
    for pattern in ['\t', '  ']:
        while pattern in text:
            text = text.replace(pattern, ' ')
    return text


def remove_eight_point(text):
    if text.startswith('8.'):
        text = text[2:]
    return text


def replace_question_with_best_guess(text):
    while '?s' in text:
        text = text.replace('?s', "'s")
    while ' ? ' in text:
        text = text.replace(' ? ', ' - ')
    while '.?' in text:
        text = text.replace('.?', "'.")
    while ',?' in text:
        text = text.replace(',?', "',")
    replace_quote, replace_hyphen = [], []
    for i, char in enumerate(text):
        if char != '?':
            continue
        # Ignore final char, assume is a question
        is_final_char = (i == len(text) - 1)
        # Ignore e.g. '? Title', but not '?. Title' or '? title'
        is_question = (i == len(text) - 2 and
                       text[i+1] == ' ' and text[i+2].isupper())
        if is_final_char or is_question:
            continue
        if i > 0 and text[i-1].isalpha() and text[i+1].isalpha():
            replace_hyphen.append(i)
        else:
            replace_quote.append(i)
    for i in reversed(replace_quote):
        text = text[:i] + "'" + text[i+1:]
    for i in reversed(replace_hyphen):
        text = text[:i] + ' - ' + text[i+1:]
    return text


def remove_trailing_exclamation(text):
    while text.endswith('!'):
        text = text[:-1]
    while text.endswith(' '):
        text = text[:-1]
    return text


def clean_text(text):
    text = remove_eight_point(text)
    text = remove_large_spaces(text)
    text = remove_generic_suffixes(text)
    text = replace_question_with_best_guess(text)
    text = remove_trailing_exclamation(text)
    text = remove_large_spaces(text)
    return text


def preprocess_row(row, orm):
    json_cols = get_json_cols(orm)
    long_text_cols = get_long_text_cols(orm)
    for k, v in row:
        if k in long_text_cols and not pd.isnull(k):
            v = upper_to_camel(v)
            v = clean_bad_chars(v) DO THIS!
        elif k in json_cols:
            v = [upper_to_camel(value) in
                 detect_and_split(v)]
        if is_nih_null(v):
            v = None
        row[k] = v
    return row
