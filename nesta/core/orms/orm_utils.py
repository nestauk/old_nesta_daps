from configparser import ConfigParser
from contextlib import contextmanager
from sqlalchemy import create_engine, insert
from sqlalchemy import exists as sql_exists
from sqlalchemy.exc import OperationalError
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import class_mapper
from sqlalchemy.sql.expression import and_, or_
from nesta.core.luigihacks.misctools import find_filepath_from_pathstub
from nesta.core.luigihacks.misctools import get_config, load_yaml_from_pathstub
from nesta.packages.misc_utils.batches import split_batches
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from datetime import datetime
from py2neo.database import Graph
import pandas as pd

import importlib
import re
import pymysql
import os
import json
import logging
import time
from collections import defaultdict
from collections.abc import Mapping


def _get_key_value(obj, key):
    """Helper method to an attribute value, dealing
    gracefully with datetimes by converting to isoformat

    Args:
        obj: Object to retrieve value from
        key (str): Name of the attribute to retrieve.
    Returns:
        {key, value}: A key-value pair corresponding to the attribute
    """
    value = getattr(obj, key)
    if isinstance(value, datetime):
        value = value.isoformat()
    return (key, value)


def orm_column_names(_class):
    """Return the set of column names for the provided ORM"""
    mapper = class_mapper(_class)
    columns = {column.key for column in mapper.columns}
    return columns


def object_to_dict(obj, shallow=False, found=None):
    """Converts a nested SqlAlchemy object to a fully
    unpacked json object.

    Args:
        obj: A SqlAlchemy object (i.e. single 'row' of data)
        shallow (bool): Fully unpack nested objs via relationships.
        found: For internal recursion, do not change the default.
    Returns:
        _obj (dict): An unpacked json-like dict object.
    """
    if found is None:  # First time
        found = set()
    # Set up the mapper and retrieve shallow values
    mapper = class_mapper(obj.__class__)
    columns = [column.key for column in mapper.columns]
    out = dict(map(lambda c: _get_key_value(obj, c), columns))
    # Shallow means ignore relationships
    relationships = {} if shallow else mapper.relationships
    for name, relation in relationships.items():
        if relation in found:  # Don't repeat relationships
            continue
        found.add(relation)
        related_obj = getattr(obj, name)
        if related_obj is None:  # Don't pursue null relations
            continue
        # Unpack flat or recursively, as required
        if relation.uselist:
            out[name] = [object_to_dict(child, found=found)
                         for child in related_obj]
        else:
            out[name] = object_to_dict(related_obj, found=found)
    return out


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
    elif not test and not key.endswith("_prod"):
        raise ValueError(err_msg.format(test=test, key=key, suffix='_prod'))
    index = config['index']
    if test and not index.endswith("_dev"):
        raise ValueError(f"In test mode the index '{key}' "
                         "must end with '_dev'")


def default_to_regular(d):
    """Convert nested defaultdicts to nested dicts.
    This is useful when you want to throw KeyErrors, which
    would be dynamically accepted otherwise.

    Args:
        d (nested defaultdict): A nested defaultdict object.
    Returns:
        _d (nested dict): A nested dict object.
    """
    if isinstance(d, defaultdict):
        d = {k: default_to_regular(v) for k, v in d.items()}
    return d


def parse_es_config(increment_version):
    """Retrieve the ES config for all endpoints and indexes,
    including auto-version-incrementing if required.

    Args:
        increment_version (bool): Move one version up? (NB: no changes to config file on disk)
    Returns:
        config: Elasticsearch config dict, for all endpoints and indexes.
    """
    raw_config = load_yaml_from_pathstub('config', 'elasticsearch.yaml')
    config = defaultdict(lambda: defaultdict(dict))
    for endpoint, endpoint_config in raw_config['endpoints'].items():
        # Build the base configuration for this endpoint
        indexes = endpoint_config.pop('indexes')
        base_config = raw_config['defaults'].copy()  # use defaults as the base...
        base_config.update(endpoint_config)          # then override with endpoint settings
        # Add the host to the config
        scheme = base_config.pop('scheme')
        _id = base_config.pop('id')
        rgn = base_config['region']
        base_config['host'] = f'{scheme}://search-{endpoint}-{_id}.{rgn}.es.amazonaws.com'
        for dataset, version in indexes.items():
            prod_idx = f'{dataset}_v' + str(version + increment_version)     # e.g. arxiv_v1 / v2
            dev_idx = f'{dataset}_dev' + ('0' if increment_version else '')  # e.g. arxiv_dev / dev0
            config[endpoint][dataset][True] = {'index': prod_idx, **base_config}  # production mode
            config[endpoint][dataset][False] = {'index': dev_idx, **base_config}  # dev mode
    return default_to_regular(config)


def setup_es(endpoint, dataset, production,
             drop_and_recreate=False, increment_version=False):
    """Retrieve the ES connection, ES config and setup the index
    if required.

    Args:
        endpoint (str): Name of the AWS ES endpoint.
        dataset (str): Name of the dataset for the ES mapping.
        production (bool): Running in production mode?
        drop_and_recreate (bool): Drop and recreate ES index?
        increment_version (bool): Move one version up?
    Returns:
        {es, es_config}: Elasticsearch connection and config dict.
    """
    es_master_config = parse_es_config(increment_version)
    es_config = es_master_config[endpoint][dataset][production]
    # Make the ES connection
    es = Elasticsearch(es_config['host'], port=es_config['port'],
                       use_ssl=True, send_get_body_as='POST')
    # Does the index already exist?
    index = es_config['index']
    exists = es.indices.exists(index=index)
    # Drop index for fresh recreation (if in test mode)
    if drop_and_recreate and (not production) and exists:
        es.indices.delete(index=index)
        exists = False
    # Create the index if required
    if not exists:
        mapping = get_es_mapping(dataset, endpoint)
        es.indices.create(index=index, body=mapping)
    return es, es_config


def get_es_ids(es, es_config, size=1000, query={}):
    '''Get all existing ES document ids for a given config

    Args:
        es: Elasticsearch connection.
        es_config (dict): Elasticsearch configuration.
    Returns:
        existing_ids (set): All existing ids
    '''
    query["_source"] = False
    scanner = scan(es, query=query,
                   index=es_config['index'],
                   doc_type=es_config['type'],
                   size=size)
    return {s['_id'] for s in scanner}

def load_json_from_pathstub(pathstub, filename, sort_on_load=True):
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
    if sort_on_load:
        _js = json.dumps(js, sort_keys=True)
        js = json.loads(_js)
    return js



def update_nested(original_dict, update_dict):
    """Update a nested dictionary with another nested dictionary.
    Has equivalent behaviour to :obj:`dict.update(self, update_dict)`.

    Args:
        original_dict (dict): The original dictionary to update.
        update_dict (dict): The dictionary from which to extract updates.
    Returns:
        original_dict (dict): The original dictionary after updates.
    """
    for k, v in update_dict.items():
        nested_dict = v
        if isinstance(v, Mapping):  # Mapping ~= any dict-like object
            nested_dict = original_dict.get(k, {})
            if nested_dict is not None:
                nested_dict = update_nested(nested_dict, v)
        original_dict[k] = nested_dict
    return original_dict


def _get_es_mapping(dataset, endpoint):
    """Sequentially apply the mappings from index, settings, the
    dataset and finally the endpoint. None of these files is strictly
    required to exist, so an endpoint could conceivably have a dataset
    unique to itself.

    Args:
        dataset (str): Name of the dataset for the ES mapping.
        endpoint (str): Name of the AWS ES endpoint.
    Returns:
        :obj:`dict`: The constructed mapping.
    """
    mapping = {}
    for _path, _prefix in [('defaults', 'defaults'),
                           ('datasets', f'{dataset}_mapping'),
                           (f'endpoints/{endpoint}', f'{dataset}_mapping')]:
        try:
            _mapping = load_json_from_pathstub(f"mappings/{_path}", f"{_prefix}.json")
        except json.JSONDecodeError as exc:
            raise ValueError(f'Could not decode "mappings/{_path}/{_prefix}.json"') from exc
        except FileNotFoundError:
            continue
        update_nested(mapping, _mapping)
    return mapping


def _apply_alias(mapping, dataset, endpoint):
    """Dynamically apply aliases to an Elasticsearch mapping. Note that
    the mapping is changed in-place.

    Args:
        mapping (dict): An ES mapping.
        dataset (str): Name of the dataset for this ES mapping.
        endpoint (str): Name of the AWS ES endpoint.
    """
    ep_path = f"mappings/endpoints/{endpoint}"
    # Load an alias, if it exists
    try:
        alias_lookup = load_json_from_pathstub(ep_path, "aliases.json")
    except FileNotFoundError:
        return
    # Check whether this is a soft or hard alias
    try:
        config = load_yaml_from_pathstub(ep_path, "config.yaml")
        hard_alias = config['hard-alias']
    except (FileNotFoundError, KeyError):
        hard_alias = False
    # Apply the aliases to the mapping properties
    propts = mapping["mappings"]["_doc"]["properties"]
    _fields = set()
    for alias, lookup in alias_lookup.items():
        if dataset not in lookup:
            continue
        field = lookup[dataset]
        propts[alias] = (propts[field] if hard_alias   # New field same as old for 'hard-alias'
                         else {"type": "alias", "path": field})  # Otherwise use an ES alias
        _fields.add(field)
    # Remove old fields if 'hard alias'
    if hard_alias:
        for f in _fields:
            propts.pop(f)


def _prune_nested(mapping):
    """Recursively remove any fields with null values from
    a nested dictionary. The input is changed in-place.

    Args:
        mapping (dict): The dictionary to prune.
    """
    for k in list(mapping.keys()):
        v = mapping[k]
        if isinstance(v, Mapping):  # Mapping ~= any dict-like
            _prune_nested(v)
        elif v is None:
            mapping.pop(k)


def get_es_mapping(dataset, endpoint):
    '''Load the ES mapping for this dataset and endpoint,
    including aliases.

    Args:
        dataset (str): Name of the dataset for the ES mapping.
        endpoint (str): Name of the AWS ES endpoint.
    Returns:
        :obj:`dict`
    '''
    mapping = _get_es_mapping(dataset, endpoint)
    _apply_alias(mapping, dataset, endpoint)
    _prune_nested(mapping)  # prunes any nested keys with null values
    return mapping


def cast_as_sql_python_type(field, data):
    """Cast the data to ensure that it is the python type expected by SQL

    Args:
        field (SqlAlchemy field): SqlAlchemy field, to cast the data
        data: A data field to be cast
    Returns:
        _data: The data field, cast as the native python equivalent of the field.
    """
    _data = field.type.python_type(data)
    if field.type.python_type is str:
        # Include the VARCHAR(n) case
        n = field.type.length if field.type.length < len(_data) else None
        _data = _data[:n]
    return _data


def get_session(db_env, section, database, Base):
    """Return a database Session instance for the given credentials,
    and also setup the table structure for the intended Base ORM.
    
    Args:
        db_env: See :obj:`get_mysql_engine`                       
        section: See :obj:`get_mysql_engine`                      
        database: See :obj:`get_mysql_engine`                     
        Base (:obj:`sqlalchemy.Base`): The Base ORM for this data.
    Returns:
        session ((:obj:`sqlalchemy.Session`): A database Session instance 
                                              for the given credentials.
    """
    engine = get_mysql_engine(db_env, section, database)
    Session = try_until_allowed(sessionmaker, engine)
    session = try_until_allowed(Session)
    try_until_allowed(Base.metadata.create_all, session.get_bind())
    return session


def filter_out_duplicates(db_env, section, database,
                          Base, _class, data,
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
        low_memory (bool): If the pkeys are few or small types (i.e. they won't
                           occupy lots of memory) then set this to True.
                           This will speed things up significantly (like x 100),
                           but will blow up for heavy pkeys or large tables.
        return_non_inserted (bool): Flag that when set will also return a lists of rows that
                                    were in the supplied data but not imported (for checks)

    Returns:
        :obj:`list` of :obj:`_class` instantiated by data, with duplicate pks removed.
    """
    session = get_session(db_env, section, database, Base)
    return _filter_out_duplicates(session, Base, _class, data, low_memory)
    return results


def get_all_pks(session, _class):
    """Get every PK in the database for this ORM"""
    pkey_cols = _class.__table__.primary_key.columns
    fields = [getattr(_class, pkey.name)
              for pkey in pkey_cols]
    all_pks = set(session.query(*fields).all())
    return all_pks


def has_auto_pkey(_class):
    """Check if the PK of the ORM is autoincrement"""
    pkey_cols = _class.__table__.primary_key.columns
    is_auto_pkey = any(p.autoincrement and
                       p.type.python_type is int
                       for p in pkey_cols)
    return is_auto_pkey


def generate_pk(row, _class):
    """Generate the PK for this row, based on the PK column names"""
    pkey_cols = _class.__table__.primary_key.columns
    pk = tuple([cast_as_sql_python_type(pkey, row[pkey.name])
                for pkey in pkey_cols])
    return pk


def _filter_out_duplicates(session, Base, _class, data,
                           low_memory=False):
    """Produce a filtered list of data, exluding duplicates and entries that
    already exist in the data.

    Args:
        session (:obj:`sqlalchemy.orm.session.Session`): SqlAlchemy session object.
        Base (:obj:`sqlalchemy.Base`): The Base ORM for this data.
        _class (:obj:`sqlalchemy.Base`): The ORM for this data.
        data (:obj:`list` of :obj:`dict`): Rows of data to insert
        low_memory (bool): If the pkeys are few or small types (i.e. they won't
                           occupy lots of memory) then set this to True.
                           This will speed things up significantly (like x 100),
                           but will blow up for heavy pkeys or large tables.
        return_non_inserted (bool): Flag that when set will also return a lists of rows that
                                    were in the supplied data but not imported (for checks)

    Returns:
        :obj:`list` of :obj:`_class` instantiated by data, with duplicate pks removed.
    """
    # Add the data
    objs = []
    existing_objs = []
    failed_objs = []
    pkey_cols = _class.__table__.primary_key.columns
    is_auto_pkey = has_auto_pkey(_class)

    # Read all pks if in low_memory mode
    all_pks = (get_all_pks(session, _class) if low_memory and not is_auto_pkey
               else set())
    for irow, row in enumerate(data):
        # The data must contain all of the pkeys
        if not is_auto_pkey and not all(pkey.name in row for pkey in pkey_cols):
            logging.warning(f"{row} does not contain any of {pkey_cols}"
                            f"{[pkey.name in row for pkey in pkey_cols]}")
            failed_objs.append(row)
            continue

        # Generate the pkey for this row
        if not is_auto_pkey:
            pk = generate_pk(row, _class)
            # The row mustn't aleady exist in the input data
            if pk in all_pks and not is_auto_pkey:
                existing_objs.append(row)
                continue
            all_pks.add(pk)
        # Nor should the row exist in the DB
        if not is_auto_pkey and not low_memory and session.query(exists(_class, **row)).scalar():
            existing_objs.append(row)
            continue
        objs.append(row)
    session.close()
    return objs, existing_objs, failed_objs


def retrieve_row_by_pk(session, pk, _class):
    """Retrieve a row from the database based on it's PK components"""
    q = session.query(_class)  # Build the base query statement
    pkey_cols = _class.__table__.primary_key.columns
    # Iterating for the case where the PK is composite
    for pk_col, pk_value in zip(pkey_cols, pk):
        q = q.filter(pk_col == pk_value)  # Append a "where" statement per PK
    obj = q.one()  # Execute the query statement
    _row = object_to_dict(obj)
    return _row


def create_delete_stmt(_class, pks):
    """Create a SqlAlchemy drop statement for fields with existing
    primary keys.
    """
    pkey_cols = _class.__table__.primary_key.columns
    # Logic massively simplifies if only one key
    is_composite = len(pkey_cols) > 1
    if is_composite:
        all_rows_stmt = []  # The overall "where" statement
        # Iterate through all existing primary keys (note, these are tuples)
        for col_values in pks:
            this_row_stmt = []  # The "where" statement for this primary key
            # Iterate through the pk name and value
            for col, value in zip(pkey_cols, col_values):
                key = getattr(_class, col.name)
                this_row_stmt.append(key == value)
            all_rows_stmt.append(and_(*this_row_stmt)) # AND the pk together
        all_rows_stmt = or_(*all_rows_stmt) # OR across rows
    else:  # Only one PK, this is much faster since can use "in_" logic
        col, = pkey_cols # Indexing isn't supported for column collections...
        key = getattr(_class, col.name)
        all_rows_stmt = key.in_(tuple(pk for pk, in pks))
    delete_stmt = _class.__table__.delete().where(all_rows_stmt)
    return delete_stmt


def is_null(x):
    """Wrapper around pandas isnull with an exception for iterables,
    which cause a ValueError on conversion to boolean."""
    try:
        return bool(pd.isnull(x))
    except ValueError:
        return False


def merge_duplicates(db_env, section, database,
                     Base, _class, data, low_memory):
    """Alternative to `filter_out_duplicates`: Find all duplicates in the list of data,
    and also for duplicates between the provided data and the database, and then
    select the most recent non-null value for upsertion.

    Args:
        session (:obj:`sqlalchemy.orm.session.Session`): SqlAlchemy session object.
        Base (:obj:`sqlalchemy.Base`): The Base ORM for this data.
        _class (:obj:`sqlalchemy.Base`): The ORM for this data.
        data (:obj:`list` of :obj:`dict`): Rows of data to insert
        low_memory (bool): If the pkeys are few or small types (i.e. they won't
                           occupy lots of memory) then set this to True.
                           This will speed things up significantly (like x 100),
                           but will blow up for heavy pkeys or large tables.
    Returns:
        :obj:`list` of :obj:`_class` instantiated by data, with duplicate pks removed.
    """
    pkey_cols = _class.__table__.primary_key.columns
    is_auto_pkey = has_auto_pkey(_class)
    if is_auto_pkey:
        raise ValueError('AutoPK fields cannot be merged, you must set merge_non_null = False')
    if not low_memory:
        raise NotImplementedError('low_memory=False mode has not been implemented for `merge_duplicates`.'
                                  'Use merge_non_null = False')

    session = get_session(db_env, section, database, Base)  # Open a session for reading the data
    all_pks = (get_all_pks(session, _class) if low_memory else set()) # Read PKs
    pks_to_drop = []  # List of PKs indicating rows to be dropped, prior to reinsertion
    pk_row_lookup = defaultdict(list)  # Grouping of rows by PK

    # Group objects into sets of duplicates
    for row in data:
        # Extract the PK for this row
        pk = generate_pk(row, _class)
        pk_row_lookup[pk].append(row)
        if pk in all_pks:  # i.e. this is a duplicate
            pks_to_drop.append(pk)  # and so will be dropped, prior to reinsertion
        else:
            continue  # i.e. not a duplicate
        # Retrieve the duplicate row
        _row = retrieve_row_by_pk(session, pk, _class)
        pk_row_lookup[pk].append(_row)
    # Generate a delete statement for duplicate rows
    # (NB: Just generating the statement here, not executing it yet)
    delete_stmt = create_delete_stmt(_class, pks_to_drop)
    session.close()

    # Now merge the fields by taking the first non-null value
    objs = []
    for pk, rows in pk_row_lookup.items():
        field_names = list(rows[0].keys())    
        merged_row = {}
        for col in field_names:
            value = None
            # Find the first non-null value for this row
            for row in rows:
                if not is_null(row[col]):
                    value = row[col]
                    break  # Take only the first non-null value
            # Assign the value for this field
            merged_row[col] = value
        objs.append(merged_row)
    return objs, delete_stmt, None


def insert_data(db_env, section, database, Base,
                _class, data, return_non_inserted=False,
                low_memory=False, merge_non_null=False,
                insert_chunksize=1000):
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
        merge_non_null (bool): Upsert rather than ignore duplicates,
                               if the updated fields are not null.
        return_non_inserted (bool): Flag that when set will also return a lists of rows that
                                were in the supplied data but not imported (for checks)

    Returns:
        :obj:`list` of :obj:`_class` instantiated by data, with duplicate pks removed.
        :obj:`list` of :obj:`dict` data found already existing in the database (optional)
        :obj:`list` of :obj:`dict` data which could not be imported (optional)
    """
    filter_function = merge_duplicates if merge_non_null else filter_out_duplicates
    response = filter_function(db_env=db_env,
                               section=section,
                               database=database,
                               Base=Base,
                               _class=_class,
                               data=data,
                               low_memory=low_memory)
    objs, existing_objs, failed_objs = response
    # Prepare for transactions
    engine = get_mysql_engine(db_env, section, database)

    # Drop existing objs if merging
    with db_session(engine) as session:
        try_until_allowed(Base.metadata.create_all, 
                          session.get_bind())
        if merge_non_null:
            session.execute(existing_objs)

    # Insert data in chunks
    for chunk in split_batches(objs, insert_chunksize):
        with db_session(engine) as session:
            stmt = insert(_class).values(chunk)
            session.execute(stmt)

    # Done
    if return_non_inserted:
        return objs, existing_objs, failed_objs
    return objs


def db_session_query(query, engine, chunksize=1000,
                     limit=None, offset=0):
    """Perform queries in chunks, with one session per chunk
    to avoid long sessions from dying.

    Args:
        query: A valid SqlAlchemy query string or object
        engine: A valid SqlAlchemy connectable
        chunksize (int): Chunk size after which to reset the db connection
        limit (int): Maximum number of results to return.
    Yields:
        {db, row} ({:obj:`sqlalchemy.orm.session.Session`, data}): SqlAlchemy session and row of data
    """
    n = 0
    n_results = chunksize
    while n_results == chunksize:
        logging.info('[db_session_query] (re)starting DB '
                     f'session after {n*chunksize + n_results}')
        with db_session(engine) as db:
            n_results = 0
            for row in (db.query(query).offset(offset + n*chunksize)
                        .limit(chunksize)):
                n_results += 1
                yield db, row
                if n*chunksize + n_results == limit:
                    return
        n += 1
    return


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
    if type(Base) is str:
        Base = get_base_from_orm_name(Base)

    for c in Base._decl_class_registry.values():
        if hasattr(c, '__tablename__') and c.__tablename__ == tablename:
            return c
    raise NameError(tablename)


def get_base_from_orm_name(orm_module_name):
    orm_module = importlib.import_module(f'nesta.core.orms.{orm_module_name}')
    return orm_module.Base


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
        url = URL(drivername='mysql+pymysql',
                  username="travis",
                  database=database)
    else:
        if not os.path.exists(conf_path):
            raise FileNotFoundError(conf_path)
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


def merge_metadata(base, *other_bases):
    """Combines the metadata from multiple declarative bases so base.metadata
    functions such as create_all can be achieved with orms kept in seperate
    files.

    Args:
        base (sqlalchemy.ext.declarative.declarative_base): main declarative
            base that the others will be merged into
        other_bases (sqlalchemy.ext.declarative.declarative_base): other
            declarative bases to be merged

    Returns:
        (sqlalchemy.ext.declarative.declarative_base): original base with the
            others merged
    """
    for b in other_bases:
        for (table_name, table) in b.metadata.tables.items():
            base.metadata._add_table(table_name, table.schema, table)
    return base


@contextmanager
def graph_session(yield_graph=False, *args, **kwargs):
    '''Generate a Neo4j graph transaction object with
    safe commit/rollback built-in.

    Args:
        {*args, **kwargs}: Any arguments for py2neo.database.Graph
    Yields:
        py2neo.database.Transaction
    '''
    graph = Graph(*args, **kwargs)
    transaction = graph.begin()
    try:
        if yield_graph:
            yield (graph, transaction)
        else:
            yield transaction
        transaction.commit()
    except:
        transaction.rollback()
        raise
    finally:
        del transaction
        del graph
