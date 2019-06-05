import logging

from nesta.production.orms.orm_utils import db_session, get_mysql_engine
from nesta.production.orms.meetup_orm import Group

from datetime import datetime as dt
import boto3
import os


def run():
    logging.getLogger().setLevel(logging.INFO)
    
    # Fetch the input parameters    
    s3_key = os.environ["BATCHPAR_key"]
    s3_bucket = os.environ["BATCHPAR_bucket"]
    start_group = int(os.environ["BATCHPAR_start_group"])
    end_group = int(os.environ["BATCHPAR_end_group"])
    members_limit = os.environ["BATCHPAR_bucket"]
    test = literal_eval(os.environ["BATCHPAR_test"])
    db_name = os.environ["BATCHPAR_db_name"]
    es_host = os.environ['BATCHPAR_outinfo']
    es_port = int(os.environ['BATCHPAR_out_port'])
    es_index = os.environ['BATCHPAR_out_index']
    es_type = os.environ['BATCHPAR_out_type']
    entity_type = os.environ["BATCHPAR_entity_type"]
    aws_auth_region = os.environ["BATCHPAR_aws_auth_region"]

    # Extract the core topics
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, batch_file)
    core_topics = set(json.loads(obj.get()['Body']._raw_stream.read()))

    field_null_mapping = load_json_from_pathstub("tier_1/field_null_mappings/",
                                                 "health_scanner.json")
    strans_kwargs={'filename':'health_meetup_groups.json',
                   'from_key':'tier_0',
                   'to_key':'tier_1',
                   'ignore':['id']}
    es = ElasticsearchPlus(hosts=es_host,
                           port=es_port,
                           aws_auth_region=aws_auth_region,
                           no_commit=("AWSBATCHTEST" in os.environ),
                           entity_type=entity_type,
                           strans_kwargs=strans_kwargs,
                           field_null_mapping=field_null_mapping,
                           null_empty_str=True,
                           coordinates_as_floats=True,
                           country_detection=True)
    
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db_name)
    with db_session(engine) as session:
        query_result = (session
                        .query(Group)
                        .filter(Group.members >= members_limit)
                        .offset(start_group)
                        .limit(end_group)
                        .all())
        for count, group in enumerate(query_result, 1):
            row = {k: v for k, v in group.__dict__.items()
                   if k in group.__table__.columns}
            row['topics'] = list(filter(lambda topic: topic in core_topics, 
                                        group.topics))
            row['urlname'] = f"https://www.meetup.com/{row['urlname']}"
            row['coordinate'] = dict(lat=row.pop('lat'), lon=row.pop('lon'))
            row['created'] = dt.fromtimestamp(row['created'])
            
            _row = es.index(index=es_index, doc_type=es_type,
                            id=row['id'], body=row_combined)
            if not count % 1000:
                logging.info(f"{count} rows loaded to elasticsearch")

    logging.info("Batch job complete.")
            



if __name__ == "__main__":
    run()
