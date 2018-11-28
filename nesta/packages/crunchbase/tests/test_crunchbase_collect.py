from unittest import mock

from nesta.packages.crunchbase.crunchbase_collect import collect_file_from_s3

BOTO3 = 'nesta.packages.crunchbase.crunchbase_collect.boto3'


@mock.patch(BOTO3)
def test_get_crunchbase_file_keys_calls_all_crunchbase_by_default(mocked_boto):
    mocked_bucket = mock.Mock()
    mocked_s3 = mock.Mock(return_value=mocked_bucket)
    mocked_boto.resource.return_value = mock.Mock(return_value=mocked_s3)


def test_get_crunchbase_file_keys_overrides_prefix():
    pass


def test_get_crunchbase_file_keys_overrides_bucket():
    pass
