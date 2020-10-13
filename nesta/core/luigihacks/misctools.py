'''
A collection of miscellaneous tools.
'''
import configparser
import os
from functools import lru_cache
import yaml
from datetime import datetime as dt


def get_config(file_name, header, path="core/config/"):
    '''Get the configuration from a file in the luigi config path
    directory, and convert the key-value pairs under the
    config :code:`header` into a `dict`.

    Parameters:
        file_name (str): The configuation file name.
        header (str): The header key in the config file.

    Returns:
        :obj:`dict`
    '''
    conf_dir_path = find_filepath_from_pathstub(path)
    conf_path = os.path.join(conf_dir_path, file_name)
    config = configparser.ConfigParser()
    config.read(conf_path)
    return dict(config[header])


def get_paths_from_relative(relative=1):
    '''A helper method for within :obj:`find_filepath_from_pathstub`.
    Prints all file and directory paths from a relative number of
    'backward steps' from the current working directory.'''
    paths = []
    for root, subdirs, files in os.walk(f"./{'../'*relative}",
                                        followlinks=True):
        # Get all directory paths
        for subdir in subdirs:
            paths.append(os.path.join(root, subdir))
        # Get all file paths
        for f in files:
            paths.append(os.path.join(root, f))
    return map(os.path.abspath, paths)


def find_filepath_from_pathstub(path_stub):
    '''Find the full path of the 'closest' file (or directory) to the current working
    directory ending with :obj:`path_stub`. The `closest` file is determined by
    starting forwards of the current working directory. The algorithm is then repeated
    by moving the current working directory backwards, one step at a time until
    the file (or directory) is found. If the HOME directory is reached, the algorithm
    raises :obj:`FileNotFoundError`.

    Args:
        path_stub (str): The partial file (or directory) path stub to find.
    Returns:
        The full path to the partial file (or directory) path stub.
    '''
    relative = 0
    while True:
        for path in get_paths_from_relative(relative):
            if path.rstrip("/") == os.environ["HOME"]:
                raise FileNotFoundError(f"Could not find {path_stub}")
            if path.endswith(path_stub.rstrip("/")):
                return path
        relative += 1


def f3p(path_stub):
    """Shortened name for coding convenience"""
    return find_filepath_from_pathstub(path_stub)


def load_yaml_from_pathstub(pathstub, filename):
    """Basic wrapper around :obj:`find_filepath_from_pathstub`
    which also opens the file (assumed to be yaml).

    Args:
        pathstub (str): Stub of filepath where the file should be found.
        filename (str): The filename.
    Returns:
        The file contents as a json-like object.
    """
    _path = find_filepath_from_pathstub(pathstub)
    _path = os.path.join(_path, filename)
    with open(_path) as f:
        return yaml.safe_load(f)


def load_batch_config(luigi_task, additional_env_files=[], **overrides):
    """Load default luigi batch parametes, and apply any overrides if required. Note that
    the usage pattern for this is normally :obj:`load_batch_config(self, additional_env_files, **overrides)`
    from within a luigi Task, where :obj:`self` is the luigi Task.
    
    Args:
        luigi_task (luigi.Task): Task to extract test and date parameters from.
        additional_env_files (list): List of files to pass directly to the batch local environment.
        overrides (**kwargs): Any overrides or additional parameters to pass to the batch task as parameters.
    Returns:
        config (dict): Batch configuration paramaters, which can be expanded as **kwargs in BatchTask.
    """
    config = load_yaml_from_pathstub('config', 'luigi-batch.yaml')
    test, routine_id = extract_task_info(luigi_task)
    config['test'] = test
    config['job_name'] = routine_id
    config['routine_id'] = routine_id
    config['env_files'] += additional_env_files
    config['env_files'] = [f3p(fp) for fp in config['env_files']]    
    config['date'] = dt.now()
    config.update(overrides)
    return config


@lru_cache()
def extract_task_info(luigi_task):
    """Extract task name and generate a routine id from a luigi task, from the date and test fields.
    
    Args:
        luigi_task (luigi.Task): Task to extract test and date parameters from.
    Returns:
        {test, routine_id} (tuple): Test flag, and routine ID for this task.
    """
    test = (luigi_task.test if 'test' in luigi_task.__dict__ 
            else not luigi_task.production)
    task_name = type(luigi_task).__name__
    routine_id = f'{task_name}-{luigi_task.date}-{test}'
    return test, routine_id


@lru_cache()
def bucket_keys(bucket_name):
    """Get all keys in an S3 bucket.
    
    Args:
        bucket_name (str): Name of a bucket to query.
    Returns:
        keys (set): Set of keys
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    keys = set(obj.key for obj in bucket.objects.all())
    return keys
