import sys
import pickle
import numpy as np
import pandas as pd
from health_keywords import health_keywords

np.random.seed(42)

orgs = pd.read_csv(sys.argv[1])
orgs['is_Health'] = orgs.category_list.apply(lambda x: 1
                                            if any(health_keyword in str(x)
                                            for health_keyword in health_keywords)
                                            else 0
                                            )
orgs = orgs[(orgs.category_group_list.isnull()==False)
            & (orgs.category_list.isnull()==False)]

data_idx = np.random.choice(orgs.index, 200000)
data = orgs.loc[data_idx, ['category_list', 'is_Health']]

with open('../data/training_data/training_data.pickle', 'wb') as h:
    pickle.dump(data, h)
