import pandas as pd
# import pytest

from nesta.packages.crunchbase.predict.src.model import train


def test_train_and_predict():
    train_data = pd.read_csv('integration_test_training_data.csv')
    assert train_data.shape == (5000, 2), 'Training data not shaped as expected'

    vec, gs, accuracy = train(train_data)
    assert accuracy == 0.997


