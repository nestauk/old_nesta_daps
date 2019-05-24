sys.argv[0]

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
for df_scratch in pd.read_sql_table('crunchbase_organizations', engine, chunksize=10000, schema='production'):

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
        column_missing_values_dict.update(dict(Counter(column_missing_values_dict) + Counter(missing_vals_count)))

        #EITHER OR AND missing pairwise
        both_dummy_dict = missing_value_count_pair_both(df_scratch)
        both_null_dict.update(dict(Counter(both_null_dict)+Counter(both_dummy_dict)))

        either_dummy_dict = missing_value_count_pair_either(df_scratch)
        either_null_dict.update(dict(Counter(either_null_dict)+Counter(either_dummy_dict)))

        j+=1

missing_value_plot(column_missing_values_dict)
missing_value_column_count_plot(column_missing_values_dict.sort_values(ascending=False))
