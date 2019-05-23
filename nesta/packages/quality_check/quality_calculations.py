
def missing_values(data_frame):

    """

    """
    return data_frame.isnull().sum().sort_values(ascending=False)

def missing_value_column_count(data_frame):

    bins_list = = [i for i in range(100+1) if i%10 == 0]
    out = pd.cut(data_frame.isnull().mean().apply(lambda x: round(100*x,5)), bins=bins_list, include_lowest=True)

    return out.value_counts(sort=False)

def missing_value_count_pair_both(data_frame):
#include here or in script itself
    pair_list = []
    for pair in itertools.combinations_with_replacement(data_frame.columns,2):
        pair_list.append(pair)
        #1min 30secs
    both_null_dict = {}
    for pair in pair_list:
        bool_1 = pd.isnull(df[pair[0]])
        bool_2 = pd.isnull(df[pair[1]])

        and_counts = (bool_1 & bool_2)
        if True in and_counts.value_counts().index:
            both_null_dict[(pair[0],pair[1])] = and_counts.sum()
            both_null_dict[(pair[1],pair[0])] = and_counts.sum()

        else:
            both_null_dict[(pair[0],pair[1])] = 0
            both_null_dict[(pair[1],pair[0])] = 0

    ser = pd.Series(list(both_null_dict.values()),
                  index=pd.MultiIndex.from_tuples(both_null_dict.keys()))
    heat_df = ser.unstack().fillna(0)

    return heat_df

def missing_value_count_pair_either(data_frame):
    pair_list = []
    for pair in itertools.combinations_with_replacement(data_frame.columns,2):
        pair_list.append(pair)

    either_null_dict = {}
for pair in pair_list:
    bool_1 = pd.isnull(df[pair[0]])
    bool_2 = pd.isnull(df[pair[1]])

    either_counts = ((bool_1 ==True) | (bool_2==True))
#     print(either_counts.value_counts())
    if True in either_counts.value_counts().index:
        either_null_dict[(pair[0],pair[1])] = either_counts.sum()
        either_null_dict[(pair[1],pair[0])] = either_counts.sum()

    else:
        either_null_dict[(pair[0],pair[1])] = 0
        either_null_dict[(pair[1],pair[0])] = 0

    ser = pd.Series(list(both_null_dict.values()),
                  index=pd.MultiIndex.from_tuples(both_null_dict.keys()))
    heat_df = ser.unstack().fillna(0)

    return heat_df
