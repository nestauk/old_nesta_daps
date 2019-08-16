import pandas as pd
import numpy as np
import itertools

def missing_values(data):
    """missing_values
    Calculates the number of missing values.

    Args:
        data (:obj:`iter` of :obj:`obj`): A sequence of objects.

    Returns:
        missing_count_df (:obj:`pandas.core.series.Series`): A series with column names (index) and missing value frequency (column)


    """
    df = pd.DataFrame(data)
    missing_count_df = df.isnull().sum().sort_values(ascending=False)
    return missing_count_df

def missing_value_percentage_column_count(data):
    """missing_value_percentage_column_count
    Calculates the number of columns with a percentage of missing values.

    Args:
        data (:obj:`pandas.DataFrame`): A sequence of objects.

    Returns:
        out_counts (:obj:`pandas.DataFrame`): A dataframe with mising value percentage ranges (index) and column frequency (column)

    """

    df = pd.DataFrame(data)
    total_length = len(df)

    missingvalue_df = missing_values(df)
    missing_bins = [i for i in range(100+1) if i%10 == 0]
    out= pd.cut((pd.Series(missingvalue_df)/total_length).apply(lambda x: round(x*100,5)),
                                                         bins=missing_bins, include_lowest=True)
    # print(missingvalue_df)
    # out = np.histogram(((pd.Series(missingvalue_df)/total_length)*100), bins=range(0, 101, 10))
    out_counts = out.value_counts(sort=False)
    out_counts_df = pd.DataFrame({'intervals': out_counts.index, 'frequency': out_counts.values})

    return out_counts_df

def missing_value_count_pair_both(data):
    """missing_value_count_pair_both

    Args:
        data (:obj:`iter` of :obj:`obj`): A multi-dimensional nested sequence of objects.

    Returns:
        both_null_dict (:obj:`pandas.DataFrame`): A symmetrical dataframe with column names (index and columns) and boolean counts if both entries of pairwise columns are missing (values)

    """
    data_frame = pd.DataFrame(data)
    pair_list = []
    for pair in itertools.combinations_with_replacement(data_frame.columns,2):
        pair_list.append(pair)

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

    ser = pd.Series(list(both_null_dict.values()),
                  index=pd.MultiIndex.from_tuples(both_null_dict.keys()))
    both_null_df = ser.unstack().fillna(0)

    return both_null_df

def missing_value_count_pair_either(data):
    """missing_value_count_pair_either

    Args:
        data (:obj:`iter` of :obj:`obj`): A multi-dimensional nested sequence of objects.

    Returns:
        either_null_dict (:obj:`pandas.DataFrame`): A symmetrical dataframe with column names (index and columns) and boolean counts if either entries of pairwise columns are missing (values)

    """

    data_frame = pd.DataFrame(data)
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

    ser = pd.Series(list(either_null_dict.values()),
                  index=pd.MultiIndex.from_tuples(either_null_dict.keys()))
    either_null_df = ser.unstack().fillna(0)

    return either_null_df
