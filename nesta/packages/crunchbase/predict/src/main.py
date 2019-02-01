import sys
import gensim
import numpy as np
import pandas as pd
# from sklearn.metrics import confusion_matrix
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer

# from utils import split_str
np.random.seed(42)


def split_str(text):
    """Split a string on comma."""
    return text.split(',')


if __name__ == '__main__':
    orgs = pd.read_csv(sys.argv[1])
    categories = [split_str(cat) for cat in list(orgs.category_list.dropna())]
    # Train a w2v and use it to find categories similar to "Health Care"
    w2v = gensim.models.Word2Vec(categories, size=350, window=10,
                                 min_count=2, iter=20)

    health_keywords = [
        'Health Care', 'Diabetes', 'Outpatient Care', 'Fertility', 'Personal Health',
        'Clinical Trials', 'Health Diagnostics', 'Emergency Medicine', 'Hospital',
        'Cosmetic Surgery', 'Therapeutics', 'Quantified Self', 'mHealth',
        'Alternative Medicine', 'Electronic Health Record (EHR)', 'Neuroscience',
        'Rehabilitation', 'Home Health Care', 'Wellness', 'Dental', 'Nutrition',
        'Dietary Supplements', 'Genetics', 'Bioinformatics', 'Health Insurance',
        'Biopharma', 'Pharmaceutical', 'Medical Device', 'Life Science',
        'Biotechnology', 'Child Care', 'Nutraceutical', 'Medical', 'Elderly',
        'Nursing and Residential Care', 'Veterinary', 'Assisted Living',
        'Cosmetics', 'Elder Care', 'Psychology', 'First Aid', 'Social Assistance',
        'Fitness', 'Public Safety'
        ]

    orgs['is_Health'] = orgs.category_list.apply(lambda x:
                                                 1 if any(health_keyword in str(x)
                                                          for health_keyword
                                                          in health_keywords)
                                                 else 0
                                                 )

    # Randomly choose a training set.
    data_idx = np.random.choice(orgs.index, 200000)
    data = orgs.loc[data_idx, ['category_list', 'is_Health']]

    # Transform the feature set to TFIDF vectors.
    vec = TfidfVectorizer(tokenizer=split_str)
    # Features & target variable
    X = vec.fit_transform(list(data['category_list']))
    y = data.is_Health

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2,
                                                        random_state=42)

    #
    clf = RandomForestClassifier(random_state=42)

    param_grid = {"max_depth": [3, None],
                  "n_estimators": [30, 100, 200],
                  "min_samples_split": [2, 3],
                  "class_weight":['balanced']}

    gs = GridSearchCV(clf, param_grid, cv=5)
    gs.fit(X_train, y_train)

    print('BEST PARAMS: {}'.format(gs.best_params_))
    print('TEST SET ACCURACY: {}'.format(gs.score(X=X_test, y=y_test)))
