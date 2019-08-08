import pandas as pd
import numpy as np
import itertools

def missing_values(input):
    """missing_values

    Args:
        input (:obj:`iter` of :obj:`obj`): A sequence of objects.

    Returns:
        missing_count_df (:obj:`pandas.core.series.Series`): A series with column names (index) and missing value frequency (column)


    """
    df = pd.DataFrame(input)
    missing_count_df = df.isnull().sum().sort_values(ascending=False)
    return missing_count_df

def missing_value_percentage_column_count(input):
    """missing_value_percentage_column_count

    Args:
        input (:obj:`dict`): A dictionary of

    Returns:
        out_counts (:obj:`pandas.core.series.Series`): A series with mising value percentage ranges (index) and column frequency (column)

    """

    missing_bins = [i for i in range(100+1) if i%10 == 0]
    out= pd.cut((pd.Series(input)/total_length).apply(lambda x: round(x*100,5)),
                                                         bins=missing_bins, include_lowest=True)
    out_counts = out.value_counts(sort=False)

    return out_counts
def missing_value_column_count(input):
    """missing_value_column_count

    Args:
        input (:obj:`iter` of :obj:`obj`): A sequence of objects.

    Returns:
        out (:obj:`tuple`): A tuple of two lists- first list with the frequency, second list the bins

    """

    df = pd.DataFrame(input)

    data_frame_missing = df.isnull().sum() / df.shape[0]
    out = np.histogram(data_frame_missing, bins=range(0, 101, 10))
    return out

def missing_value_count_pair_both(input):
    """missing_value_count_pair_both

    Args:
        input (:obj:`iter` of :obj:`obj`): A multi-dimensional nested sequence of objects.

    Returns:
        both_null_dict (:obj:`tuple`): A tuple of two lists- first list with the frequency, second list the bins

    """
    data_frame = pd.DataFrame(input)
    pair_list = []
    for pair in itertools.combinations_with_replacement(data_frame.columns,2):
        pair_list.append(pair)
        #1min 30secs
    both_null_dict = {}
    for pair in pair_list:
        bool_1 = pd.isnull(data_frame[pair[0]])
        bool_2 = pd.isnull(data_frame[pair[1]])

        and_counts = (bool_1 & bool_2)
        if True in and_counts.value_counts().index:
            both_null_dict[(pair[0],pair[1])] = and_counts.sum()
            both_null_dict[(pair[1],pair[0])] = and_counts.sum()

        else:
            both_null_dict[(pair[0],pair[1])] = 0
            both_null_dict[(pair[1],pair[0])] = 0

    return both_null_dict

def missing_value_count_pair_either(input):
    """missing_value_count_pair_either

    Args:
        input (:obj:`iter` of :obj:`obj`): A multi-dimensional nested sequence of objects.

    Returns:
        either_null_dict (:obj:`tuple`): A tuple of two lists- first list with the frequency, second list the bins

    """

    data_frame = pd.DataFrame(input)
    pair_list = []
    for pair in itertools.combinations_with_replacement(data_frame.columns,2):
        pair_list.append(pair)

    either_null_dict = {}
    for pair in pair_list:
        bool_1 = pd.isnull(data_frame[pair[0]])
        bool_2 = pd.isnull(data_frame[pair[1]])

        either_counts = ((bool_1 ==True) | (bool_2==True))
    #     print(either_counts.value_counts())
        if True in either_counts.value_counts().index:
            either_null_dict[(pair[0],pair[1])] = either_counts.sum()
            either_null_dict[(pair[1],pair[0])] = either_counts.sum()

        else:
            either_null_dict[(pair[0],pair[1])] = 0
            either_null_dict[(pair[1],pair[0])] = 0

    return either_null_dict

def melt_dict_to_df(calc_dict):
    """melt_dict_to_df

    Args:
        calc_dict (:obj:`dict`): Dictionary object with 2-element tuple keys and 1-dimensional values.

    Returns:
        df (:obj:`tuple`): A dataframe with the elements of the tuple keys as indices and columns and the values as the dictionary values.

    """
    ser = pd.Series(list(calc_dict.values()),
                  index=pd.MultiIndex.from_tuples(calc_dict.keys()))
    df = ser.unstack().fillna(0)

    return df
