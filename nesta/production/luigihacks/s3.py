# -*- coding: utf-8 -*-
"""
A more recent implementation of AWS S3 support, stolen from: https://gitlab.com/ced/s3_helpers/blob/master/luigi_s3_target.py,
but instead using modern boto3 commands.
"""

try:
    from urlparse import urlsplit
except:
    from urllib.parse import urlsplit

import boto3
from botocore.exceptions import ClientError

from luigi import configuration
from luigi.format import get_default_format
from luigi.target import FileAlreadyExists, FileSystem, FileSystemTarget, AtomicLocalFile


S3_DIRECTORY_MARKER_SUFFIX = '/'


def merge_dicts(*dicts):
    "Merge dicts together, with later entries overriding earlier ones."
    merged = {}
    for d in dicts:
        merged.update(d)
    return merged

def parse_s3_path(path):
    "For a given S3 path, return the bucket and key values"
    parsed_path = urlsplit(path)
    s3_bucket = parsed_path.netloc
    s3_key = parsed_path.path.lstrip('/')
    return (s3_bucket, s3_key)



class S3FS(FileSystem):
    def __init__(self, **kwargs):
        luigi_s3_config = self._get_s3_client_config()
        s3_config = merge_dicts(kwargs, luigi_s3_config) 
        self.s3 = boto3.resource('s3', **s3_config)
        self.s3_client = boto3.client('s3', **s3_config)

    def _get_s3_client_config(self):
        defaults = dict(configuration.get_config().defaults())
        try:
            config = dict(configuration.get_config().items('s3'))
        except NoSectionError:
            return {}
        return config

    def _is_root(self, path):
        (s3_bucket, s3_key) = parse_s3_path(path)
        if not s3_key or s3_key == S3_DIRECTORY_MARKER_SUFFIX:
            return True
        return False

    def _add_path_delimiter(self, key):
        if not key or key.endswith(S3_DIRECTORY_MARKER_SUFFIX):
            return key
        else:
            return key + S3_DIRECTORY_MARKER_SUFFIX

    def exists(self, path):
        "Return true if S3 key exists"
        # Root path always exists
        if self._is_root(path):
            return True

        (s3_bucket, s3_key) = parse_s3_path(path)
        s3_obj = self.s3.Object(s3_bucket, s3_key)
        try:
            s3_obj.reload()
        except ClientError as err:
            if err.response['Error']['Message'] == 'Not Found':
                return False
            else:
                raise
        return True

    def remove(self, path, recursive=True):
        "Remove a file or directory from S3"

        if self._is_root(path):
            raise InvalidDeleteException('Cannot delete root of bucket at path {0}'.format(path))

        (s3_bucket, s3_key) = parse_s3_path(path)
        s3_obj = self.s3.Object(s3_bucket, s3_key)
        if not self.exists(path):
            logger.debug('Could not delete %s; path does not exist', path)
            return False
        s3_obj.delete()
        logger.debug('Deleting %s from bucket %s', s3_key, s3_bucket)
        return True

    def mkdir(self, path, parents=True, raise_if_exists=False):
        if self._is_root(path):
            return

        if raise_if_exists and self.isdir(path):
            raise FileAlreadyExists()

        (s3_bucket, s3_key) = parse_s3_path(path)
        s3_obj = self.s3.Object(s3_bucket, s3_key)

        return s3_obj.put(Body=b'')

    def isdir(self, path):
        (s3_bucket, s3_key) = parse_s3_path(path)
        if not s3_key or s3_key.endswith(S3_DIRECTORY_MARKER_SUFFIX):
            return True

        # If there's keys under it, it's a dir
        response = self.s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)
        if response['Contents']:
            return True

        return False

    def listdir(self, path):
        (s3_bucket, s3_key) = parse_s3_path(path)
        key_path_len = len(s3_key)
        response = self.s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)
        return [obj['Key'][key_path_len:] for obj in response['Contents']]

    def copy(self, path, dest):
        (source_s3_bucket, source_s3_key) = parse_s3_path(path)
        (dest_s3_bucket, dest_s3_key) = parse_s3_path(dest)
        dest_s3_obj = self.s3.Object(dest_s3_bucket, dest_s3_key)
        return dest_s3_obj.copy({'Bucket': source_s3_bucket, 'Key': source_s3_key})

    def move(self, path, dest):
        self.copy(path, dest)
        self.remove(path)



class S3Target(FileSystemTarget):
    fs = None

    def __init__(self, path, s3_args={}, **kwargs):
        super(S3Target, self).__init__(path)
        self.path = path
        (self.s3_bucket, self.s3_key) = parse_s3_path(self.path)
        self.fs = S3FS(**s3_args)
        self.s3_obj = self.fs.s3.Object(self.s3_bucket, self.s3_key)
        self.s3_obj_options = kwargs

    def open(self, mode='rb'):
        if mode not in ('rb', 'wb'):
            raise ValueError("Unsupported open mode '{0}'".format(mode))

        if mode == 'rb':
            return self.s3_obj.get()['Body']
        else:
            return AtomicS3File(self.path, self.s3_obj, **self.s3_obj_options)



class AtomicS3File(AtomicLocalFile):
    def __init__(self, path, s3_obj, **kwargs):
        self.s3_obj = s3_obj
        super(AtomicS3File, self).__init__(path)
        self.s3_obj_options = kwargs

    def move_to_final_destination(self):
        self.s3_obj.upload_file(self.tmp_path, **self.s3_obj_options)
