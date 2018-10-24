from configparser import ConfigParser
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import pymysql
import os
from sqlalchemy.exc import OperationalError
from elasticsearch import Elasticsearch
import json


def get_class_by_tablename(Base, tablename):
  """Return class reference mapped to table.

  Args:
      tablename (str): Name of table.

  Returns:
      reference or None.
  """
  for c in Base._decl_class_registry.values():
      if hasattr(c, '__tablename__') and c.__tablename__ == tablename:
          return c


def try_until_allowed(f, *args, **kwargs):
    '''Keep trying a function if a specific exception is raised.
    Specifically meant for handling too many connections to a database.

    Args:
        f (:obj:`function`): A function to keep trying.
    '''
    while True:
        try:
            value = f(*args, **kwargs)
        except OperationalError:
            logging.warning("Waiting on exception {}".format(exception))
            time.sleep(5)
            continue
        else:
            return value


def get_mysql_engine(db_env, section, database="production_tests"):
    '''Generates the MySQL DB engine for tests
    
    Args:
        db_env (str): Name of environmental variable 
                      describing the path to the DB config.
        section (str): Section of the DB config to use.
        database (str): Which database to use 
                        (default is a database called 'production_tests')
    '''

    conf_path = os.environ[db_env]
    if conf_path == "TRAVISMODE":
        print("---> In travis mode")
        url = URL(drivername='mysql+pymysql',
                  username="travis",
                  database=database)
    else:
        cp = ConfigParser()
        cp.read(conf_path)
        conf = dict(cp._sections[section])
        url = URL(drivername='mysql+pymysql',
                  username=conf['user'],
                  password=conf['password'],
                  host=conf['host'],
                  port=conf['port'],
                  database=database)
    # Create the database
    return create_engine(url, connect_args={"charset":"utf8mb4"})


def get_elasticsearch_config(es_env, section):
    '''Loads local configuration for elasticsearch.

    Args:
        es_env (str): name of the environmental variable holding the path to the config
        section (str): section of the document holding the relevent configuration

    Returns:
        (dict): settings for elasticsearch
    '''
    conf_path = os.environ[es_env]
    cp = ConfigParser()
    cp.read(conf_path)
    conf = dict(cp._sections[section])
    es_config = {'host': conf['host'],
                 'port': conf['port'],
                 'index': conf['index'],
                 'type': conf['type']
                 }
    return es_config


def create_elasticsearch_index(index_name, es_client, config_path=None):
    '''Wrapper for the elasticsearch library to create indices.

    Args:
        index_name (str): name of the new index
        es_client (object): es client for the targer cluster
        config_path (str): local path to the .json file containing the mapping and settings

    Returns:
        acknowledgement from the cluster
    '''
    config = None
    if config_path:
        with open(config_path) as f:
            config = json.load(f)
    response = es_client.indices.create(index=index_name, body=config)
    print(response)
    return response
