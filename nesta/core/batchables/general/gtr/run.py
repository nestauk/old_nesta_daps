"""
run.py (GtR general)
--------------------

Transfer pre-collected GtR data from MySQL to Elasticsearch.
"""

from ast import literal_eval
import boto3
import json
import logging
import os
from datetime import datetime as dt

from nesta.core.luigihacks.elasticsearchplus import ElasticsearchPlus
from nesta.core.luigihacks.luigi_logging import set_log_level
from nesta.core.orms.orm_utils import db_session, get_mysql_engine
from nesta.core.orms.orm_utils import load_json_from_pathstub
from nesta.core.orms.orm_utils import object_to_dict, get_class_by_tablename
from nesta.core.orms.gtr_orm import Base, Projects, LinkTable, OrganisationLocation
from collections import defaultdict


def default_pop(dictobj, key, default={}):
    try:
        default = dictobj.pop(key)
    except KeyError:
        pass
    return default


def extract_funds(gtr_funds):
    """Extract and deduplicate funding information

    Args:
        gtr_funds (list of dict): Raw GtR funding information for a single project
    Returns:
        _gtr_funds (list of dict): Deduplicated GtR funding information, ready for ingestion to ES
    """
    funds = {}
    for row in gtr_funds:
        row = {k:row[k] for k in row if k != 'id'}
        row['start_date'] = row.pop('start')
        row['end_date'] = row.pop('end')
        composite_key = (row[k] for k in ('start_date', 'end_date', 'category',
                                          'amount', 'currencyCode'))
        funds[tuple(composite_key)] = row
    return [row for _, row in funds.items()]


def get_linked_rows(session, links):
    """Pull rows out of the database from various tables,
    as indicated by the link table.

    Args:
        session (SqlAlchemy session): Open session from which to query the database.
        links (dict): Mapping of table name to a list of PKs in that table
    Returns:
        rows (dict): Mapping of table name to a list of rows of data from that table
    """
    linked_rows = defaultdict(list)
    for table_name, ids in links.items():
        _class = get_class_by_tablename(Base, table_name)
        if table_name.startswith('gtr_outcomes'):
            table_name = 'gtr_outcomes'
        linked_rows[table_name] += [object_to_dict(_obj)
                                    for _obj in (session.query(_class)\
                                                 .filter(_class.id.in_(ids))\
                                                 .all())]
    return linked_rows


def reformat_row(row, linked_rows, locations):
    """Prepare raw data for ingestion to ES.

    Args:
        row (dict): Row of data.
        linked_rows (dict): Mapping of table name to a list of rows of data from that table
        locations (dict): Mapping of organisation id to location data
    Returns:
        row (dict): Reformatted row of data
    """
    # Extract general info
    gtr_funds = default_pop(linked_rows, 'gtr_funds')
    row['funds'] = extract_funds(gtr_funds)
    row['outcomes'] = linked_rows['gtr_outcomes']
    row['topics'] = [r['text'] for r in linked_rows['gtr_topic'] if r['text'] != 'Unclassified']
    row['institutes'] = [r['name'] for r in linked_rows['gtr_organisations']]
    row['institute_ids'] = [r['id'] for r in linked_rows['gtr_organisations']]

    # Extract geographic info
    org_ids = list(row['institute_ids'])
    _locations = [loc for org_id, loc in locations.items() if org_id in org_ids]
    row['countries'] = [loc['country_name'] for loc in _locations]
    row['country_alpha_2'] = [loc['country_alpha_2'] for loc in _locations]
    row['continent'] = [loc['continent'] for loc in _locations]

    row['locations'] = []
    for loc in _locations:
        lat = loc['latitude']
        lon = loc['longitude']
        if lat is None or lon is None:
            continue
        row['locations'].append({'lat': float(lat), 'lon': float(lon)})
    return row


def get_project_links(session, project_ids):
    """Generate the look-up table of table_name to object ids, by project id, 
    as a prepatory stage for retrieving the "rows" by id from each table_name, 
    by project id.

    Args:
        session (SqlAlchemy session): Open session from which to query the database.
        project_ids (list-like): List of project ids to extract linked entities from.
    Returns:
        linked_rows (dict): Mapping of table name to a list of row ids of data in that table
    """
    project_links = defaultdict(lambda: defaultdict(list))
    for obj in session.query(LinkTable).filter(LinkTable.project_id.in_(project_ids)).all():
        row = object_to_dict(obj)
        project_links[row['project_id']][row['table_name']].append(row['id'])
    return project_links


def get_org_locations(session):
    """Retrieve look-up of all organisation ids to location metadata.

    Args:
        session (SqlAlchemy session): Open session from which to query the database.
    Returns:
        locations (nested dict): Mapping of organisation id to location metadata.
    """
    locations = {}
    for obj in session.query(OrganisationLocation).all():
        row = object_to_dict(obj)
        locations[row.pop('id')] = row
    return locations


def run():
    test = literal_eval(os.environ["BATCHPAR_test"])
    bucket = os.environ['BATCHPAR_bucket']
    batch_file = os.environ['BATCHPAR_batch_file']
    db_name = os.environ["BATCHPAR_db_name"]
    es_host = os.environ['BATCHPAR_outinfo']
    es_port = int(os.environ['BATCHPAR_out_port'])
    es_index = os.environ['BATCHPAR_out_index']
    entity_type = os.environ["BATCHPAR_entity_type"]
    aws_auth_region = os.environ["BATCHPAR_aws_auth_region"]

    # database setup
    logging.info('Retrieving engine connection')
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb",
                              db_name)

    # es setup
    logging.info('Connecting to ES')
    strans_kwargs = {'filename': 'gtr.json', 'ignore': ['id']}
    es = ElasticsearchPlus(hosts=es_host,
                           port=es_port,
                           aws_auth_region=aws_auth_region,
                           no_commit=("AWSBATCHTEST" in
                                      os.environ),
                           entity_type=entity_type,
                           strans_kwargs=strans_kwargs,
                           null_empty_str=True,
                           coordinates_as_floats=True,
                           listify_terms=True,
                           do_sort=False,
                           ngram_fields=['textBody_abstract_project',
                                         'textBody_potentialImpact_project',
                                         'textBody_techAbstract_project'])

    # collect file
    logging.info('Retrieving project ids')
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, batch_file)
    project_ids = json.loads(obj.get()['Body']._raw_stream.read())
    logging.info(f"{len(project_ids)} project IDs "
                 "retrieved from s3")

    #
    logging.info('Processing rows')
    with db_session(engine) as session:
        locations = get_org_locations(session)
        project_links = get_project_links(session, project_ids)
        for count, obj in enumerate((session.query(Projects)
                                     .filter(Projects.id.in_(project_ids))
                                     .all())):
            row = object_to_dict(obj)
            links = default_pop(project_links, row['id'])
            linked_rows = get_linked_rows(session, links)
            row = reformat_row(row, linked_rows, locations)
            es.index(index=es_index, id=row.pop('id'), body=row)
            if not count % 1000:
                logging.info(f"{count} rows loaded to "
                             "elasticsearch")


if __name__ == "__main__":
    set_log_level()
    if 'BATCHPAR_outinfo' not in os.environ:
        from nesta.core.orms.orm_utils import setup_es
        from nesta.core.luigihacks.misctools import find_filepath_from_pathstub
        es, es_config = setup_es(production=False, endpoint='general',
                                 dataset='gtr', drop_and_recreate=True)
        environ = {'config': find_filepath_from_pathstub('mysqldb.config'),
                   'batch_file' : (''),
                   'db_name': 'dev',
                   'bucket': 'nesta-production-intermediate',
                   'outinfo': es_config['host'],
                   'out_port': es_config['port'],
                   'out_index': es_config['index'],
                   'aws_auth_region': 'eu-west-2',
                   'entity_type': 'project',
                   'test': "True"}
        for k, v in environ.items():
            os.environ[f'BATCHPAR_{k}'] = v

    logging.info('Starting...')
    run()
