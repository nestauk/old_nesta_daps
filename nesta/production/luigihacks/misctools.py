'''
A collection of miscellaneous tools.
'''

import configparser
import os


def get_config(file_name, header):
    '''Get the configuration from a file in the LUIGI_CONFIG_PATH
    directory, and convert the key-value pairs under the config :code:`header`
    into a `dict`.

    Parameters:
        file_name (str): The configuation file name.
        header (str): The header key in the config file.

    Returns:
        :obj:`dict`
    '''
    conf_dir_path, _ = os.path.split(os.environ["LUIGI_CONFIG_PATH"])
    conf_path = os.path.join(conf_dir_path, file_name)
    config = configparser.ConfigParser()
    config.read(conf_path)
    return dict(config[header])
