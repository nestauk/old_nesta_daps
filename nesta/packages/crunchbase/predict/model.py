"""Training a vectoriser and random forest classifier model, based on a labeled training
dataset. This is primarily designed for health labeling of crunchbase organisatons.
"""
import logging
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import GridSearchCV, train_test_split

from nesta.packages.crunchbase.utils import split_str


def train(data, random_seed=42):
    """Consumes a training dataset, with binary labels and trains a vectoriser and
    random forests classifier.

    Args:
        data (:obj:`pandas.DataFrame`)
        random_seed (int): seed for any randomisers

    Returns:
        vectoriser model
        classifier model
    """
    # Transform the feature set to TFIDF vectors
    vec = TfidfVectorizer(tokenizer=split_str)

    # Features & target variable
    X = vec.fit_transform(list(data['category_list']))
    y = data.is_health

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2,
                                                        random_state=random_seed)

    # Training
    clf = RandomForestClassifier(random_state=random_seed)

    param_grid = {"max_depth": [3, None],
                  "n_estimators": [30, 100, 200],
                  "min_samples_split": [2, 3],
                  "class_weight": ['balanced']}

    gs = GridSearchCV(clf, param_grid, cv=5)
    gs.fit(X_train, y_train)

    accuracy = gs.score(X=X_test, y=y_test)

    logging.info(f"BEST PARAMS: {gs.best_params_}")
    logging.info(f"TEST SET ACCURACY: {accuracy}")
    logging.info(f"CONFUSION MATRIX:\n{confusion_matrix(y_test, gs.predict(X_test))}")

    return vec, gs, accuracy


if __name__ == '__main__':
    import pickle
    import sys

    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")

    vec_out = '../models/vectoriser.pickle'
    clf_out = '../models/clf.pickle'

    with open(sys.argv[1], 'rb') as h:
        data = pickle.load(h)

    vec, gs, _ = train(data)

    with open(vec_out, 'wb') as h:
        pickle.dump(vec, h)

    with open(clf_out, 'wb') as h:
        pickle.dump(gs, h)
