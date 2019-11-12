import pandas as pd
import itertools
from collections import Counter
##string length
def string_length(s, include_nan= False):
    '''string_length
    Calculates the character-length of strings.

    Args:
        s (:obj:`iter` of :obj:`str`): A sequence of string objects.
        include_nan (:obj:`str`): Option to count NaN values

    Returns:
        flat_length_series (:obj:`pandas.core.series.Series`): A series onject of string lengths.
    '''
    if include_nan == False:
        series = pd.Series(s)

        flat_length_list = series[~pd.isnull(series)].str.len().values
        flat_length_series = pd.Series(flat_length_list)

    elif include_nan == True:
        series = pd.Series(s)
        series.fillna('', inplace= True)
        flat_length_list = series[~pd.isnull(series)].str.len().values
        flat_length_series = pd.Series(flat_length_list)

    return flat_length_series


def string_counter(s):
    '''string_counter
    Count of unique strings in dataset.

    Args:
        s (:obj:`iter` of :obj:`list`): A sequence of string objects.

    Returns:
        count_output (:obj:`pandas.core.series.Series`): A series object of strings and string freuqencies.
    '''

    series = pd.Series(s)
    count = Counter(series)
    count = (series.value_counts()
                     .reset_index()
                     .rename(columns={'index': 'Text', series.name: 'Frequency'}))
    # count_output = pd.Series(count)
    return count#_output
