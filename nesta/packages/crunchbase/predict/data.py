"""
Creates a training dataset by applying a boolean field based on the presence of keywords
within the data. This is primarily designed for use with the crunchbase organisation
data but should be applicable to other similar processes.
"""
import numpy as np


def label_data(data, keywords, column, label_name, size, random_seed=42):
    """Adds a boolean column to a dataframe based on the value of a columns and a list
    of keywords. A sample of the original data is returned.

    Args:
        data (:obj: `pandas.DataFrame`): dataframe containing the column to label
        keywords (list): keywords to check for
        column (str): target column in the dataframe
        label_name (str): column name to use for the boolean
        size (int): sample number of records from data
        random_seed (int): seed for the numpy random generator

    Returns:
        (:obj: `pandas.DataFrame): original data with label added as an additonal column
    """
    np.random.seed(random_seed)

    data[label_name] = data[column].apply(lambda x: int(any(k in str(x)
                                                            for k in keywords)))

    data_idx = np.random.choice(data.index, size, replace=False)
    data = data.loc[data_idx]

    return data


if __name__ == '__main__':
    import json
    import pandas as pd
    import pickle
    import sys

    org_file = sys.argv[1]
    orgs = pd.read_csv(org_file)
    orgs = orgs[~orgs.category_list.isnull()]

    with open('health_keywords.json') as f:
        health_keywords = json.load(f)

    labeled = label_data(orgs, health_keywords, 'category_list', 'is_health', 200000)
    labeled = labeled[['category_list', 'is_health']]

    with open('../data/training_data/training_data.pickle', 'wb') as h:
        pickle.dump(labeled, h)
