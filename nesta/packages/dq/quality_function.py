from quality_calculations import *
from quality_plots import *
from collections import Counter
import pandas as pd


def quality_check(data, stream = None, table = None, schema = None, engine = None, chunk = None):
    if stream == True and data == 'stream':
    # i = 0
        total_length = 0

        #missing values
        column_missing_values_dict = {}

        #missing value distributions
        missing_bins = [i for i in range(100+1) if i%10 == 0]

        #data type distib count
        data_type_count = {}

        #
        pair_list = []
        both_null_dict = {}
        either_null_dict = {}

        j=0
        for df_scratch in pd.read_sql_table(table, engine, chunksize=chunk, schema=schema):

            if j == 0:

                total_length += len(df_scratch)

                #missing values
                column_missing_values_dict = missing_values(df_scratch)


                #EITHER OR AND missing pairwise
                both_null_dict = missing_value_count_pair_both(df_scratch)

                either_null_dict = missing_value_count_pair_either(df_scratch)



                j+=1

            else:

                total_length += len(df_scratch)

                #missing values
                missing_vals_count = missing_values(df_scratch)
                column_missing_values_dict = column_missing_values_dict + missing_vals_count


                # #EITHER OR AND missing pairwise
                both_dummy_dict = missing_value_count_pair_both(df_scratch)
                both_null_dict.update(dict(Counter(both_null_dict)+Counter(both_dummy_dict)))
                #
                either_dummy_dict = missing_value_count_pair_either(df_scratch)
                either_null_dict.update(dict(Counter(either_null_dict)+Counter(either_dummy_dict)))
                #
                j+=1

        missing_value_plot(column_missing_values_dict.sort_values(ascending=False), total_length)
        missing_value_column_count_plot(missing_value_column_count(pd.DataFrame(column_missing_values_dict)))
        missing_value_count_pair_plot(melt_dict_to_df(both_null_dict))
        missing_value_count_pair_plot(melt_dict_to_df(either_null_dict))

    else:
        #missing values data
        missing_val_df = missing_values(data)
        missing_value_plot(missing_val_df, len(data))

        #missing distribution
        missing_distib = missing_value_column_count(data)
        missing_value_column_count_plot(missing_distib)

        #EITHER OR AND missing pairwise
        both_null_dict = melt_dict_to_df(missing_value_count_pair_both(data))
        either_null_dict = melt_dict_to_df(missing_value_count_pair_either(data))

        missing_value_count_pair_plot(both_null_dict)
        missing_value_count_pair_plot(either_null_dict)