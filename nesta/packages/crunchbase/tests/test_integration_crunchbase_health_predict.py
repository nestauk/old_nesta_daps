import json
import os
import numpy as np
import pandas as pd
import pickle

from nesta.packages.crunchbase.predict.model import train
from nesta.packages.crunchbase.crunchbase_collect import predict_health_flag


TEST_PATH = os.path.dirname(__file__)
TRAINING_DATA = f'{TEST_PATH}/integration_test_training_data.csv'
UNLABELED_DATA = f'{TEST_PATH}/integration_test_dataset_to_label.json'
EXPECTED_LABELED_DATA = f'{TEST_PATH}/integration_test_expected_labeled_dataset.json'


def test_train_and_predict():
    # collect training data
    train_data = pd.read_csv(TRAINING_DATA)
    assert train_data.shape == (100, 2), "Training data not shaped as expected. Should just contain 'category_list' and 'is_health' columns"

    # train and against expected confusion matrix
    vec, gs, confusion_matrix = train(train_data)
    expected_confusion_matrix = np.array([[17, 0], [2, 1]], np.int64)
    np.testing.assert_array_equal(confusion_matrix, expected_confusion_matrix)

    # pickle and unpickle, as per the implementation (catches errors with lambda)
    pickled_vec = pickle.dumps(vec)
    pickled_gs = pickle.dumps(gs)
    vec = pickle.loads(pickled_vec)
    gs = pickle.loads(pickled_gs)

    # collect unlabeled and expected data
    with open(UNLABELED_DATA) as f:
        unlabeled_data = json.load(f)
    with open(EXPECTED_LABELED_DATA) as f:
        expected_data = json.load(f)

    # predict and check labels
    labeled_data = predict_health_flag(unlabeled_data, vec, gs)
    assert labeled_data == expected_data
