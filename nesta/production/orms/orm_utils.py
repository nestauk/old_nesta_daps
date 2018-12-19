from configparser import ConfigParser
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy import exists as sql_exists
from sqlalchemy.exc import OperationalError
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import and_

import pymysql
import os

import json

import logging
import time


def insert_data(db_env, section, database, Base, _class, data, return_non_inserted=False):
    """
    Convenience method for getting the MySQL engine and inserting
    data into the DB whilst ensuring a good connection is obtained
    and that no duplicate primary keys are inserted.
    Args:
        db_env: See :obj:`get_mysql_engine`
        section: See :obj:`get_mysql_engine`
        database: See :obj:`get_mysql_engine`
        Base (:obj:`sqlalchemy.Base`): The Base ORM for this data.
        _class (:obj:`sqlalchemy.Base`): The ORM for this data.
        data (:obj:`list` of :obj:`dict`): Rows of data to insert
        return_non_inserted (bool): Flag that when set will also return a lists of rows that
                                were in the supplied data but not imported (for checks)
    Returns:
        :obj:`list` of :obj:`_class` instantiated by data, with duplicate pks removed.
        :obj:`list` of :obj:`dict` data found already existing in the database (optional)
        :obj:`list` of :obj:`dict` data which could not be imported (optional)
    """
    engine = get_mysql_engine(db_env, section, database)
    try_until_allowed(Base.metadata.create_all, engine)
    Session = try_until_allowed(sessionmaker, engine)
    session = try_until_allowed(Session)
    # Add the data
    all_pks = set()
    objs = []
    existing_objs = []
    failed_objs = []
    pkey_cols = _class.__table__.primary_key.columns
    for row in data:
        # The data must contain all of the pkeys
        if not all(pkey.name in row for pkey in pkey_cols):
            logging.warning(f"{row} does not contain any of "
                            "{[pkey.name in row for pkey in pkey_cols]}")
            failed_objs.append(row)
            continue
        # The row mustn't already exist in the db data
        if session.query(exists(_class, **row)).scalar():
            existing_objs.append(row)
            continue
        # The row mustn't aleady exist in the input data
        pk = tuple([row[pkey.name] for pkey in pkey_cols])
        if pk in all_pks:
            existing_objs.append(row)
            continue
        all_pks.add(pk)
        objs.append(_class(**row))
    logging.debug(f"all objects: {objs}")
    logging.debug(f"all existing objects: {existing_objs}")
    session.bulk_save_objects(objs)
    session.commit()
    session.close()
    if return_non_inserted:
        return objs, existing_objs, failed_objs
    return objs


@contextmanager
def db_session(engine):
    Session = try_until_allowed(sessionmaker, engine)
    session = try_until_allowed(Session)
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def exists(_class, **kwargs):
    """Generate a sqlalchemy.exists statement for a generic ORM
    based on the primary keys of that ORM.

    Args:
         _class (:obj:`sqlalchemy.Base`): A sqlalchemy ORM
         **kwargs (dict): A row of data containing the primary key fields and values.
    Returns:
         :code:`sqlalchemy.exists` statement.
    """
    statements = [getattr(_class, pkey.name) == kwargs[pkey.name]
                  for pkey in _class.__table__.primary_key.columns]
    return sql_exists().where(and_(*statements))


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
    '''Keep trying a function if a OperationalError is raised.
    Specifically meant for handling too many
    connections to a database.

    Args:
        f (:obj:`function`): A function to keep trying.
    '''
    while True:
        try:
            value = f(*args, **kwargs)
        except OperationalError:
            logging.warning("Waiting on OperationalError")
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
    return create_engine(url, connect_args={"charset": "utf8mb4"})


def create_elasticsearch_index(es_client, index, config_path=None):
    '''Wrapper for the elasticsearch library to create indices.

    Args:
        es_client (object): es client for the targer cluster
        index_name (str): name of the new index
        config_path (str): local path to the .json file containing the mapping and settings

    Returns:
        acknowledgement from the cluster
    '''
    config = None
    if config_path:
        with open(config_path) as f:
            config = json.load(f)
    response = es_client.indices.create(index=index, body=config)
    return response
