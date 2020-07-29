import pytest

from unittest.mock import patch
from nesta.packages.misc_utils.s3_utils import pickle_to_s3


@patch("nesta.packages.misc_utils.s3_utils.boto3")
def test_pickle_to_s3(boto3):
    pickle_to_s3("test_data", "bucket", "prefix")

    boto3.resource.assert_called_with("s3")
    boto3.resource().Object.assert_called_with("bucket", "prefix.pickle")
