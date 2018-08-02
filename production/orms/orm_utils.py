from configparser import ConfigParser
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import os

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
    return create_engine(url)
    
