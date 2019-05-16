from configparser import ConfigParser
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy import exists as sql_exists
from sqlalchemy.exc import OperationalError
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import and_
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub
from nesta.production.luigihacks.misctools import get_config
from elasticsearch import Elasticsearch

import pymysql
import os
import json
import logging
import time

def assert_correct_config(test, config, key):
    """Assert that config key and 'index' value are consistent with the
    running mode.    
    
    Args:
        test (bool): Are we running in test mode?
        config (dict): Elasticsearch config file data.
        key (str): The name of the config key.
    """
    err_msg = ("In test mode='{test}', config key '{key}' "
               "must end with '{suffix}'")
    if test and not key.endswith("_dev"):
        raise ValueError(err_msg.format(test=test, key=key, suffix='_dev'))
    elif not self.test and not index_name.endswith("_prod"):
        raise ValueError(err_msg.format(test=test, key=key, suffix='_prod'))
    index = config[key]['index']
    if test and not index.endswith("_dev"):
        raise ValueError(f"In test mode the index '{key}' "
                         "must end with '_dev'")


def setup_es(es_mode, test_mode, reindex_mode, dataset, aliases=None):
    """Retrieve the ES connection, ES config and setup the index 
    if required.
    
    Args:
        es_mode (str): One of "prod" or "dev".
        test_mode (bool): Running in test mode?
        reindex_mode (bool): Running in reindex mode?
        dataset (str): Name of the dataset for the ES mapping.
        aliases (str): Name of the aliases for the ES mapping.
    Returns:
        {es, es_config}: Elasticsearch connection and config dict.
    """
    if es_mode not in ("prod", "dev"):
        raise ValueError("es_mode required to be one of "
                         f"'prod' or 'dev', but '{es_mode}' provided.")

    # Get and check the config
    es_config = get_config('elasticsearch.config', es_mode)
    assert_correct_config(test_mode, es_mode, es_config)    
    # Make the ES connection
    es = Elasticsearch(es_config['host'], port=es_config['port'], 
                       use_ssl=True)
    # Drop the index if required (must be in test mode to do this)         
    _index = es_config['index']
    if reindex_mode and test_mode:
        es.indices.delete(index=_index)
    # Create the index if required
    exists = es.indices.exists(index=_index)
    if not exists:
        mapping = get_es_mapping(dataset, aliases=aliases)
        es.indices.create(index=_index, body=mapping)
    return es, es_config


def load_json_from_pathstub(pathstub, filename):
    """Basic wrapper around :obj:`find_filepath_from_pathstub`
    which also opens the file (assumed to be json).
    
    Args:
        pathstub (str): Stub of filepath where the file should be found.
        filename (str): The filename.
    Returns:
        The file contents as a json object.
    """
    _path = find_filepath_from_pathstub(pathstub)
    _path = os.path.join(_path, filename)
    with open(_path) as f:
        js = json.load(f)
    return js

def get_es_mapping(dataset, aliases):
    '''Get the configuration from a file in the luigi config path
    directory, and convert the key-value pairs under the config :code:`header`
    into a `dict`.

    Parameters:
        file_name (str): The configuation file name.
        header (str): The header key in the config file.

    Returns:
        :obj:`dict`
    '''
    # Get the mapping and lookup
    mapping = load_json_from_pathstub("production/orms/",
                                      f"{dataset}_es_config.json")
    alias_lookup = load_json_from_pathstub("production/tier_1/aliases/",
                                           f"{aliases}.json")
    # Get a list of valid fields for verification
    fields = mapping["mappings"]["_doc"]["properties"].keys()
    # Add any aliases to the mapping
    for alias, lookup in alias_lookup.items():
        if dataset not in lookup:
            continue
        # Validate the field
        field = lookup[dataset]
        if field not in fields:
            raise ValueError(f"Alias '{alias}' to '{field}' but '{field}'"
                             "does not exist in the mapping.")
        # Add the alias to the mapping
        value = {"type": "alias", "path": lookup[dataset]}
        mapping["mappings"]["_doc"]["properties"][alias] = value
    return mapping


def filter_out_duplicates(db_env, section, database, Base, _class, data,
                          low_memory=False):
    """Produce a filtered list of data, exluding duplicates and entries that
    already exist in the data.

    Args:
        db_env: See :obj:`get_mysql_engine`
        section: See :obj:`get_mysql_engine`
        database: See :obj:`get_mysql_engine`
        Base (:obj:`sqlalchemy.Base`): The Base ORM for this data.
        _class (:obj:`sqlalchemy.Base`): The ORM for this data.
        data (:obj:`list` of :obj:`dict`): Rows of data to insert
        low_memory (bool): To speed things up significantly, you can read
                           all pkeys into memory first, but this will blow
                           up for heavy pkeys or large tables.
        return_non_inserted (bool): Flag that when set will also return a lists of rows that
                                    were in the supplied data but not imported (for checks)

    Returns:
        :obj:`list` of :obj:`_class` instantiated by data, with duplicate pks removed.
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

    # Read all pks if in low_memory mode
    if low_memory:
        fields = [getattr(_class, pkey.name) for pkey in pkey_cols]
        all_pks = set(session.query(*fields).all())

    for irow, row in enumerate(data):
        # The data must contain all of the pkeys
        if not all(pkey.name in row for pkey in pkey_cols):
            logging.warning(f"{row} does not contain any of "
                            "{[pkey.name in row for pkey in pkey_cols]}")
            failed_objs.append(row)
            continue

        # Generate the pkey for this row
        pk = tuple([row[pkey.name]                       # Cast to str if required, since
                    if pkey.type.python_type is not str  # pandas may have incorrectly guessed
                    else str(row[pkey.name])             # the type as int
                    for pkey in pkey_cols])

        # The row mustn't aleady exist in the input data
        if pk in all_pks:
            existing_objs.append(row)
            continue
        all_pks.add(pk)
        # Nor should the row exist in the DB
        if not low_memory and session.query(exists(_class, **row)).scalar():
            existing_objs.append(row)
            continue
        objs.append(_class(**row))
    session.close()
    return objs, existing_objs, failed_objs


def insert_data(db_env, section, database, Base, _class, data, return_non_inserted=False, low_memory=False):
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
        low_memory (bool): To speed things up significantly, you can read
                           all pkeys into memory first, but this will blow
                           up for heavy pkeys or large tables.
        return_non_inserted (bool): Flag that when set will also return a lists of rows that
                                were in the supplied data but not imported (for checks)

    Returns:
        :obj:`list` of :obj:`_class` instantiated by data, with duplicate pks removed.
        :obj:`list` of :obj:`dict` data found already existing in the database (optional)
        :obj:`list` of :obj:`dict` data which could not be imported (optional)
    """

    objs, existing_objs, failed_objs = filter_out_duplicates(db_env=db_env, section=section,
                                                             database=database,
                                                             Base=Base, _class=_class, data=data,
                                                             low_memory=low_memory)

    # save and commit
    engine = get_mysql_engine(db_env, section, database)
    try_until_allowed(Base.metadata.create_all, engine)
    Session = try_until_allowed(sessionmaker, engine)
    session = try_until_allowed(Session)
    session.bulk_save_objects(objs)
    session.commit()
    session.close()
    if return_non_inserted:
        return objs, existing_objs, failed_objs
    return objs


@contextmanager
def db_session(engine):
    """Creates and mangages an sqlalchemy session.

    Args:
        engine (:obj:`sqlalchemy.engine.base.Engine`): engine to use to access the database

    Returns:
        (:obj:`sqlalchemy.orm.session.Session`): generated session
    """
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
    raise NameError(tablename)


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
