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
from nesta.core.orms.orm_utils import object_to_dict
from nesta.core.orms.gtr_orm import Base, Projects, LinkTable, OrganisationLocation
from collections import defaultdict 

 
def extract_funds(gtr_funds):  
    funds = {}  
    for row in gtr_funds:  
        row = {k:row[k] for k in row if k != 'id'}  
        row['start_date'] = row.pop('start') 
        row['end_date'] = row.pop('end')         
        composite_key = (row[k] for k in ('start_date', 'end_date', 'category', 
                                          'amount', 'currencyCode'))         
        funds[tuple(composite_key)] = row         
    return [row for _, row in funds.items()]  


def get_linked_rows(links):
    linked_rows = defaultdict(list)
    for table_name, ids in links.items():
        _class = get_class_by_tablename(Base, table_name)
        if table_name.startswith('gtr_outcomes'):
            table_name = 'gtr_outcomes'
        linked_rows[table_name] += [object_to_dict(_obj)
                                    for _obj in (session.query(_class)\
                                                 .filter(_class.id.in_(ids))\
                                                 .all())]

def reformat_row(row):
    """Prepare raw data for ingestion to ES.

    Args:
        row (dict): Row of data.
    Returns:
        row (dict): Reformatted row of data
    """
    # Extract general info
    row['funds'] = extract_funds(linked_rows.pop('gtr_funds')) 
    row['outcomes'] = linked_rows['gtr_outcomes'] 
    row['topics'] = [r['text'] for r in linked_rows['gtr_topic'] if r['text'] != 'Unclassified']
    row['institutes'] = [r['name'] for r in linked_rows['gtr_organisations']] 
    row['institute_ids'] = [r['id'] for r in linked_rows['gtr_organisations']] 

    # Extract geographic info
    org_ids = set(row['institute_ids']) - set(locations.keys())
    row['countries'] = [locations[org_id]['country_name'] for org_id in org_ids]
    row['country_alpha_2'] = [locations[org_id]['country_alpha_2'] for org_id in org_ids]
    row['continent'] = [locations[org_id]['continent'] for org_id in org_ids]
    row['locations'] = [{'lat': float(locations[org_id]['latitude']), 
                         'lon': float(locations[org_id]['longitude'])} for org_id in org_ids]
    return row


def get_project_links(project_ids):
    project_links = defaultdict(lambda: defaultdict(list)) 
    for obj in session.query(LinkTable).filter(LinkTable.project_id.in_(project_ids)).all():
        row = object_to_dict(obj)        
        project_links[row['project_id']][row['table_name']].append(row['id'])
    return project_links


def get_org_locations():
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
        locations = get_org_locations()
        project_links = get_project_links(project_ids)        
        for count, obj in enumerate((session.query(Projects)
                                     .filter(Projects.id.in_(project_ids))
                                     .all())):
            row = object_to_dict(row)
            linked_rows = get_linked_rows(project_links.pop(row['id']))
            row = reformat_row(row, links, locations)            
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
