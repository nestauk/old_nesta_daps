import json
import pandas as pd

from nesta.packages.crunchbase.predict.src.model import train
from nesta.packages.crunchbase.crunchbase_collect import predict_health_flag


# ******resolve this before merge...path needs to work on remote machines ********
# TEST_PATH = 'nesta/packages/crunchbase/tests'
TEST_PATH = 'tests'
TRAINING_DATA = f'{TEST_PATH}/integration_test_training_data.csv'
UNLABELED_DATA = f'{TEST_PATH}/integration_test_dataset_to_label.json'
EXPECTED_LABELED_DATA = f'{TEST_PATH}/integration_test_expected_labeled_dataset.json'


def test_train_and_predict():
    train_data = pd.read_csv(TRAINING_DATA)
    assert train_data.shape == (5000, 2), 'Training data not shaped as expected'

    # train and check accuracy
    vec, gs, accuracy = train(train_data)
    assert accuracy == 0.997

    # collect unlabeled and expected data
    with open(UNLABELED_DATA) as f:
        unlabeled_data = json.load(f)
    with open(EXPECTED_LABELED_DATA) as f:
        expected_data = json.load(f)

    # predict and check labels
    labeled_data = predict_health_flag(unlabeled_data, vec, gs)
    assert labeled_data == expected_data
