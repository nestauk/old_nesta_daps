import logging
import lxml  # To force pipreqs' hand

from nesta.packages.geo_utils.geocode import generate_composite_key
from nesta.packages.geo_utils.country_iso_code import country_iso_code_to_name

from nesta.core.luigihacks.elasticsearchplus import ElasticsearchPlus
from nesta.core.orms.orm_utils import db_session, get_mysql_engine
from nesta.core.orms.meetup_orm import Group
from nesta.core.orms.geographic_orm import Geographic
from nesta.packages.meetup.meetup_utils import get_members_by_percentile
from nesta.core.orms.orm_utils import load_json_from_pathstub

from bs4 import BeautifulSoup
import json
from ast import literal_eval
from datetime import datetime as dt
import boto3
import os
import requests

def run():

    # Fetch the input parameters
    s3_bucket = os.environ["BATCHPAR_bucket"]
    batch_file = os.environ["BATCHPAR_batch_file"]
    members_perc = int(os.environ["BATCHPAR_members_perc"])
    db_name = os.environ["BATCHPAR_db_name"]
    es_host = os.environ['BATCHPAR_outinfo']
    es_port = int(os.environ['BATCHPAR_out_port'])
    es_index = os.environ['BATCHPAR_out_index']
    es_type = os.environ['BATCHPAR_out_type']
    entity_type = os.environ["BATCHPAR_entity_type"]
    aws_auth_region = os.environ["BATCHPAR_aws_auth_region"]
    routine_id = os.environ["BATCHPAR_routine_id"]

    # Get continent lookup
    url = "https://nesta-open-data.s3.eu-west-2.amazonaws.com/rwjf-viz/continent_codes_names.json"
    continent_lookup = {row["Code"]: row["Name"] for row in requests.get(url).json()}
    continent_lookup[None] = None

    # Extract the core topics
    logging.DEBUG('Getting topics')
    s3 = boto3.resource('s3')
    topics_key = f'meetup-topics-{routine_id}.json'
    topics_obj = s3.Object(s3_bucket, topics_key)
    core_topics = set(json.loads(topics_obj.get()['Body']._raw_stream.read()))

    # Extract the group ids for this task
    ids_obj = s3.Object(s3_bucket, batch_file)
    group_ids = set(json.loads(ids_obj.get()['Body']._raw_stream.read()))

    field_null_mapping = load_json_from_pathstub("tier_1/field_null_mappings/",
                                                 "health_scanner.json")
    strans_kwargs={'filename':'meetup.json',
                   'from_key':'tier_0',
                   'to_key':'tier_1',
                   'ignore':[]}
    es = ElasticsearchPlus(hosts=es_host,
                           port=es_port,
                           aws_auth_region=aws_auth_region,
                           no_commit=("AWSBATCHTEST" in os.environ),
                           entity_type=entity_type,
                           strans_kwargs=strans_kwargs,
                           field_null_mapping=field_null_mapping,
                           null_empty_str=True,
                           coordinates_as_floats=True,
                           country_detection=True,
                           auto_translate=True)

    # Generate the lookup for geographies
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db_name)
    geo_lookup = {}
    with db_session(engine) as session:
        query_result = session.query(Geographic).all()
        for geography in query_result:
            geo_lookup[geography.id] = {k: v for k, v in geography.__dict__.items()
                                        if k in geography.__table__.columns}

    # Pipe the groups
    members_limit = get_members_by_percentile(engine, perc=members_perc)
    with db_session(engine) as session:
        query_result = (session
                        .query(Group)
                        .filter(Group.members >= members_limit)
                        .filter(Group.id.in_(group_ids))
                        .all())
        for count, group in enumerate(query_result, 1):
            row = {k: v for k, v in group.__dict__.items()
                   if k in group.__table__.columns}

            # Filter groups without the required topics
            topics = [topic['name'] for topic in group.topics
                      if topic['name'] in core_topics]
            if len(topics) == 0:
                continue

            # Get the geographic data for this row
            country_name = country_iso_code_to_name(row['country'], iso2=True)
            geo_key = generate_composite_key(row['city'], country_name)
            geo = geo_lookup[geo_key]

            # Clean up the input data
            row['topics'] = topics
            row['urlname'] = f"https://www.meetup.com/{row['urlname']}"
            row['coordinate'] = dict(lat=geo['latitude'], lon=geo['longitude'])
            row['created'] = dt.strftime(dt.fromtimestamp(row['created']/1000), 
                                         format="%Y-%m-%d")
            if row['description'] is not None:
                row['description'] = BeautifulSoup(row['description'], 'lxml').text                
            row['continent'] = continent_lookup[geo['continent']]
            row['country_name'] = geo['country']
            row['continent_id'] = geo['continent']
            row['country'] = geo['country_alpha_2']
            row['iso3'] = geo['country_alpha_3']
            row['isoNumeric'] = geo['country_numeric']

            # Insert to ES
            _row = es.index(index=es_index, doc_type=es_type,
                            id=row['id'], body=row)
            if not count % 1000:
                logging.info(f"{count} rows loaded to elasticsearch")

    logging.info("Batch job complete.")

# For local debugging
if __name__ == "__main__":

    log_level = logging.INFO
    if 'BATCHPAR_outinfo' not in os.environ:
        log_level = logging.VERBOSE
        environ = {'batch_file': ('2019-07-17-community-environment'
                                  '--health-wellbeing'
                                  '--fitness'
                                  '-10-99-True-'
                                  '15633764681888585.json'),
                    'config': ('/home/ec2-user/nesta/nesta/core/'
                               'config/mysqldb.config'),
                    'db_name': 'production',
                    'bucket': 'nesta-production-intermediate',
                    'outinfo': ('https://search-health-scanner'
                                '-5cs7g52446h7qscocqmiky5dn4.'
                                'eu-west-2.es.amazonaws.com'),
                    'out_port': '443',
                    'out_index': 'meetup_v2',
                    'out_type': '_doc',
                    'aws_auth_region': 'eu-west-2',
                    'entity_type': 'meetup',
                    'members_perc': '10',
                    'routine_id': ('2019-07-17-community-environment-'
                                   '-health-wellbeing--fitness-'
                                   '10-99-True')}

        for k, v in environ.items():
            os.environ[f"BATCHPAR_{k}"] = v
        #os.environ["AWSBATCHTEST"] = ""

    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=log_level,
                        format="%(asctime)s:%(levelname)s:%(message)s")
    run()
