from nesta.packages.cordis.cordis_api import fetch_data
from nesta.core.orms.cordis_orm import Base
from nesta.core.orms.orm_utils import get_mysql_engine
from nesta.core.orms.orm_utils import try_until_allowed
from nesta.core.orms.orm_utils import get_class_by_tablename
from nesta.core.orms.orm_utils import insert_data

import logging
import os
import json
import boto3
from collections import defaultdict


def extract_core_orgs(orgs, project_rcn):
    core_orgs = []
    for org in orgs:
        ctry = org.pop('country')
        core_orgs.append({'name': org.pop('name'),
                          'id': org['organization_id'],
                          'country_code': ctry['isoCode'],
                          'country_name': ctry['name']})
    return core_orgs


def prepare_data(items, rcn):
    return [dict(project_rcn=rcn, **item) for item in items]


def split_links(items, project_rcn):
    for item in items:
        yield ({'title': item['title'], 'rcn': item['rcn']},
               {'project_rcn': project_rcn, 'rcn': item['rcn']})


def run():
    batch_file = os.environ['BATCHPAR_batch_file']
    bucket = os.environ['BATCHPAR_bucket']
    db_name = os.environ['BATCHPAR_db_name']    
    db_env = "BATCHPAR_config"
    db_section = "mysqldb"

    # Setup the database connectors
    engine = get_mysql_engine(db_env, db_section, db_name)
    #Base.metadata.drop_all(engine)
    try_until_allowed(Base.metadata.create_all, engine)

    # Retrieve RCNs to iterate over
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, batch_file)
    all_rcn = json.loads(obj.get()['Body']._raw_stream.read())
    logging.info(f"{len(all_rcn)} project RCNs retrieved from s3")

    # Retrieve all topics
    data = defaultdict(list)
    for i, rcn in enumerate(all_rcn):
        project, orgs, reports, pubs = fetch_data(rcn)
        _topics = project.pop('topics')
        _calls = project.pop('proposal_call')
        # NB: Order below matters due to FK constraints!
        data['projects'].append(project)
        data['reports'] += prepare_data(reports, rcn)
        data['publications'] += prepare_data(pubs, rcn)
        data['organisations'] += extract_core_orgs(orgs, rcn)
        data['project_organisations'] += prepare_data(orgs, rcn)
        for topics, project_topics in split_links(_topics, rcn):
            data['topics'].append(topics)
            data['project_topics'].append(project_topics)
        for calls, project_calls in split_links(_calls, rcn):
            data['proposal_calls'].append(calls)
            data['project_proposal_calls'].append(project_calls)

    # Pipe the data to the db
    for table_prefix, rows in data.items():
        table_name = f'cordis_{table_prefix}'
        logging.info(table_name)
        _class = get_class_by_tablename(Base, table_name)
        insert_data(db_env, db_section, db_name, Base,
                    _class, rows, low_memory=True)

if __name__ == "__main__":
    
    if 'BATCHPAR_config' not in os.environ:
        from nesta.core.luigihacks.luigi_logging import set_log_level
        set_log_level(True)
        os.environ['BATCHPAR_batch_file'] = ('Cordis-2019-08-29-False-1567092984546124.json')
        os.environ['BATCHPAR_db_name'] = 'dev'
        os.environ["BATCHPAR_config"] = ('/home/ec2-user/'
                                         'nesta/nesta/core/config/'
                                         'mysqldb.config')
        os.environ["BATCHPAR_bucket"] = ('nesta-production'
                                         '-intermediate')
    run()
