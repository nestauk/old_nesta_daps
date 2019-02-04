import sys
import pickle
import numpy as np
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from nesta.packages.crunchbase.utils import split_str


def train(data, vec_out='../models/vectoriser.pickle', clf_out='../models/clf.pickle'):
    """Transform and train.

    Args:
        data (:obj:`unpickled training dataset`)
    """
    np.random.seed(42)

    # Transform the feature set to TFIDF vectors.
    vec = TfidfVectorizer(tokenizer=split_str)
    # Features & target variable
    X = vec.fit_transform(list(data['category_list']))
    y = data.is_Health

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2,
                                                        random_state=42)

    # Training
    clf = RandomForestClassifier(random_state=42)

    param_grid = {"max_depth": [3, None],
                  "n_estimators": [30, 100, 200],
                  "min_samples_split": [2, 3],
                  "class_weight": ['balanced']}

    gs = GridSearchCV(clf, param_grid, cv=5)
    gs.fit(X_train, y_train)

    print('BEST PARAMS: {}'.format(gs.best_params_))
    print('TEST SET ACCURACY: {}'.format(gs.score(X=X_test, y=y_test)))
    print('CONFUSION MATRIX:\n{}'.format(confusion_matrix(y_test, gs.predict(X_test))))

    with open(vec_out, 'wb') as h:
        pickle.dump(vec, h)

    with open(clf_out, 'wb') as h:
        pickle.dump(gs, h)


if __name__ == "__main__":
    with open(sys.argv[1], 'rb') as h:
        data = pickle.load(h)
    train(data)
