"""
preprocess_nih
==============

Data cleaning / wrangling before ingestion of raw data,
specifically:

  * Systematically removing generic prefixes using very hard-coded logic.
  * Inferring how to correctly deal with mystery question marks,
    using very hard-coded logic.
  * Splitting of strings into arrays as indicated by JSON the ORM,
  * CAPS to Camel Case for any string field which isn't VARCHAR(n) < 10
  * Dealing consistently with null values
"""
from sqlalchemy.dialects.mysql import VARCHAR
from sqlalchemy.types import JSON
from functools import lru_cache
import pandas as pd
import string

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
    """Return the column names in the ORM which are of JSON type"""
    for col in orm.__table__.columns:
        print(col.name, type(col.type))
    return {col.name for col in orm.__table__.columns
            if type(col.type) is JSON}


@lru_cache()
def get_long_text_cols(orm, min_length=10):
    """Return the column names in the ORM which are a text type,
    (i.e. TEXT or VARCHAR) and if a max length is specified, with max 
    length > 10. The length requirement is because we don't want 
    to preprocess ID-like or code fields (e.g. ISO codes).
    """
    return {col.name for col in orm.__table__.columns
            if col.type.python_type is str and
            not (type(col.type) == VARCHAR  and col.type.length < 10)}


def is_nih_null(value, nulls=('', [], {}, 'N/A', 'Not Required', 'None')):
    """Returns True if the value is listed in the `nulls` argument,
    or the value is NaN, null or None."""
    try:
        iter(value)
    # Only run pd.isnull if the value is not iterable (list, dict, etc)
    except TypeError:
        is_null = pd.isnull(value)
    # Otherwise set a dummy value = False
    else:
        is_null = False
    # Then check if value is either null, or in the list of null values
    finally:        
        return is_null or value in nulls


@lru_cache()
def expand_prefix_list():
    """Expand GENERIC_PREFIXES to include integers, and then a large
    numbers of permutations of additional characters, upper case 
    and title case. From tests, this covers 19 out of 20 generic
    prefixes from either abstract text or the "PHR" field."""
    prefixes = []
    numbers = list(str(i) for i in range(0, 10))
    for prefix in GENERIC_PREFIXES + numbers:
        _prefixes = [prefix]
        _prefixes += [prefix+ext for ext in ['', '/', ':', '-', ' -', '.']]
        _prefixes += [ext+prefix for ext in ['?']] 
        prefixes += _prefixes
    prefixes += [p+' ' for p in prefixes]
    prefixes += [p.upper() for p in prefixes]
    prefixes += [p.title() for p in prefixes]
    # Sort by the longest first
    return sorted(set(prefixes), key=lambda x: len(x), reverse=True)


def remove_generic_suffixes(text):
    """Iteratively remove any of the generic terms in `expand_prefix_list`
    from the front of the text, until none remain."""
    still_replacing = True
    while still_replacing:
        still_replacing = False
        for prefix in expand_prefix_list():  # NB: lru_cached
            if not text.startswith(prefix):
                continue
            text = text.replace(prefix, '', 1)
            still_replacing = True
            break
    return text


def remove_large_spaces(text):
    """Iteratively replace any large spaces or tabs with a single space,
    until none remain."""
    for pattern in ['\t', '  ']:
        while pattern in text:
            text = text.replace(pattern, ' ')
    while text.startswith(' '):
        text = text[1:]
    while text.endswith(' '):
        text = text[:-1]    
    return text


def replace_question_with_best_guess(text):
    """Somewhere in NiH's backend, they have a unicode processing problem.
    From inspection, most of the '?' symbols have quite an intuitive origin,
    and so this function contains the hard-coded logic for inferring
    what symbol used to be in the place of each '?'.
    """
    # Straightforward find and replace
    for find, replace in [('?s', "'s"), (' ? ', ' - '),
                          ('.?', "'."), (',?', "',"),
                          ('n?t', "n't")]:
        if find in text:
            text = text.replace(find, replace)
        if find.upper() in text:
            text = text.replace(find.upper(), replace.upper())

    # Most '?' will be replaced with a single quote,
    # though some will be replaced by a hyphen.
    replace_quote, replace_hyphen = set(), set()
    # Iterate through chars in text to find those that should be changed
    for i, char in enumerate(text):
        if char != '?':
            continue
        # Ignore final char, assume is a question
        is_final_char = (i == len(text) - 1)
        # Ignore e.g. '? Title', but not '?. Title' or '? title'
        is_question = (i < len(text) - 2 and  # a char exists after '? '
                       text[i+1] == ' ' and text[i+2].isupper())        
        if is_final_char or is_question:
            continue
        # The case 'sometext?somemoretext' --> 'sometext - somemoretext'
        if i > 0 and text[i-1].isalpha() and text[i+1].isalpha():
            replace_hyphen.add(i)
        # Everything else
        else:
            replace_quote.add(i)

    # Replace from back-to-front, so that the indexes don't become shuffled
    for i in reversed(range(len(text))):
        if i in replace_quote:
            text = text[:i] + "'" + text[i+1:]
        elif i in replace_hyphen:
            text = text[:i] + ' - ' + text[i+1:]
    return text


def remove_trailing_exclamation(text):
    """A lot of abstracts end with '!' and then a bunch of spaces."""
    while text.endswith('!'):
        text = text[:-1]
    return text


def upper_to_title(text):
    """Inconsistently, NiH has fields as all upper case. 
    Convert to titlecase"""
    if text == text.upper():
        text = string.capwords(text.lower())
    return text


def clean_text(text, suffix_removal_length=100):
    """Apply the full text-cleaning procedure."""
    operations = [remove_large_spaces, remove_trailing_exclamation]
    if len(text) > suffix_removal_length:
        operations.append(remove_generic_suffixes)
    operations += [replace_question_with_best_guess, upper_to_title]
    # Sequentially apply operations, and clean up spaces
    # after each operation
    for f in operations:
        text = f(text)
        text = remove_large_spaces(text)
    return text

def detect_and_split(value):
    n_commas = value.count(",")
    n_colons = value.count(";")
    # e.g "last_name, first_name; next_last_name, next_first_name"
    if n_colons >= n_commas - 1:  # also includes case where n=0
        value = value.split(";")    
    else:
        value = value.split(",")        
    return value


def preprocess_row(row, orm):
    """Clean text, split values and standardise nulls, as required.
    
    Args:
        row (dict): Row of data to clean, that should match 
                    the provided ORM.
        orm (SqlAlchemy selectable): ORM from which to infer JSON 
                                     and text fields.
    """
    json_cols = get_json_cols(orm)
    long_text_cols = get_long_text_cols(orm)
    for col_name, col_value in row.copy().items():
        if col_name in long_text_cols and not pd.isnull(col_value):
            col_value = clean_text(col_value)
        elif col_name in json_cols:
            col_value = [upper_to_title(value)
                         for value in detect_and_split(col_value)]
        if is_nih_null(col_value):
            col_value = None
        row[col_name] = col_value
    return row
