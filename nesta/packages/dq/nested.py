##arrays
def array_length(input, column, type_):
    '''array_length
    Args:
        input (:obj:`iter` of :obj:`list`): A sequence of list objects.
        column (:obj:`list`): A list of column name strings.
        type_ (:obj:`str`): A string of the type of column in question.

    Returns:
        array_length (:obj:`list`): An array of array lengths.
    '''
    series = pd.Series(input)
    # array_length = dataframe[column].apply(lambda x: len(x) if type(x) == type_ else None).values
    array_length = series.apply(lambda x: len(x) if type(x) == type_ else None)

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
    output = [j for j in series if type(j) == list]

    return output

def word_array_calc(input, calculation_type):
    '''
    Args:
        input (:obj:`list`):
        calculation_type (:obj:`str`): A string indicaing which type of calculation to perform.

    Returns:
        output1 (:obj:`list`): A list of array lengths.
        output2 (:obj:`Counter`): Counter object of counts.

    '''

    if calculation_type == 'word_length':
        output1 = [len(j) for j in itertools.chain(*input)]
        return output1

    elif calculation_type == 'count':
        output2 = Counter(itertools.chain(*input))
        return output2
