import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from nesta.packages.crunchbase.predict.data import label_data


@pytest.fixture
def data():
    return pd.DataFrame({'id': [1, 2, 3],
                         'target': ['cat', 'dog', 'log'],
                         'other': ['stuff', 'things', 'wings']})


@pytest.fixture
def keywords():
    return ['dog', 'cat', 'frog']


def test_label_applied_correctly(data, keywords):
    labeled_data = label_data(data, keywords, 'target', 'label', 3)

    expected_data = pd.DataFrame({'id': [1, 2, 3],
                                  'target': ['cat', 'dog', 'log'],
                                  'other': ['stuff', 'things', 'wings'],
                                  'label': [1, 1, 0]})

    assert_frame_equal(labeled_data, expected_data, check_like=True)


def test_sample_limiting(data, keywords):
    labeled_data = label_data(data, keywords, 'target', 'label', 2)
    assert len(labeled_data) == 2

    labeled_data = label_data(data, keywords, 'target', 'label', 1)
    assert len(labeled_data) == 1
