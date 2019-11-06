import pandas as pd
import itertools
from collections import Counter
##arrays

def keep_lists(input):
    '''keep_lists
    Helps filter out instances which are not lists.

    Args:
        input (:obj: ``)

    '''

    return [i for i in input if type(i) == list]

def array_length(input):
    '''array_length
    Calculates the lenth of arrays.

    Args:
        input (:obj:`iter` of :obj:`list`): A sequence of list objects.
        column (:obj:`list`): A list of column name strings.
        type_ (:obj:`str`): A string of the type of column in question.

    Returns:
        array_length (:obj:`pandas.core.series.Series`): An array of array lengths.
    '''
    series = pd.Series(input)
    # array_length = dataframe[column].apply(lambda x: len(x) if type(x) == type_ else None).values
    array_length = series.apply(lambda x: len(x) if type(x) == list else None)

    return array_length

def word_arrays(input):
    '''array_length


    Args:
        input (:obj:`iter` of :obj:`list`): A sequence of list objects.

    Returns:
        output (:obj:`list`): An array of lists.
    '''
    #this only keeps the list values and ignore NaNs etc
    series = pd.Series(input)
    output = keep_lists(series)

    return output

def word_array_calc(input, calculation_type == 'word_length'):
    '''word_array_calc
    Multifunctional function which calculates either the lengths of the list's tokens or the frequency of the  tokens that appears in a list.

    Args:
        input (:obj:`list`):
        calculation_type (:obj:`str`): A keyword argument which calculates word length ('word_length' by default) or count (frequencies) of tokens.

    Returns:
        word_length_output (:obj:`list`): A list of array lengths.
        count_output (:obj:`Counter`): Counter object of counts.

    '''

    if calculation_type == 'word_length':
        list_input = keep_lists(input)
        output = [len(j) for j in itertools.chain(*list_input)]
        word_length_output = pd.Series(output)
        return word_length_output

    elif calculation_type == 'count':
        list_input = keep_lists(input)
        output = Counter(itertools.chain(*list_input))
        count_output = pd.Series(output)
        return count_output
