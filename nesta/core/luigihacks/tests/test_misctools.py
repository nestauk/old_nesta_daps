from unittest import TestCase, mock
from nesta.core.luigihacks.misctools import get_config
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub
from nesta.core.luigihacks.misctools import bucket_keys


class TestMiscTools(TestCase):
    def test_get_config(self):
        get_config("mysqldb.config", "mysqldb")
        with self.assertRaises(KeyError):
            get_config("mysqldb.config", "invalid")
        with self.assertRaises(KeyError):
            get_config("not_found.config", "mysqldb")

    def test_find_filepath_from_pathstub(self):
        find_filepath_from_pathstub("nesta/packages")
        with self.assertRaises(FileNotFoundError):
            find_filepath_from_pathstub("nesta/package")


@mock.patch('nesta.core.luigihacks.misctools.boto3')
def test_bucket_keys(mocked_boto3):
    keys = {'foo', 'bar', 'baz'}

    # Mock up the bucket
    bucket_objs = []
    for key in keys:
        obj = mock.Mock()
        obj.key = key
        bucket_objs.append(obj)
    mocked_bucket = mocked_boto3.resource().Bucket()
    mocked_bucket.objects.all.return_value = bucket_objs
    
    # Actually do the test
    assert bucket_keys('dummy') == keys
