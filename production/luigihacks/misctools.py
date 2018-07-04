import configparser
import os

def get_config(file_name, header):
    conf_dir_path, _ = os.path.split(os.environ["LUIGI_CONFIG_PATH"])
    conf_path = os.path.join(conf_dir_path, file_name)
    config = configparser.ConfigParser()
    config.read(conf_path)
    return dict(config[header])

