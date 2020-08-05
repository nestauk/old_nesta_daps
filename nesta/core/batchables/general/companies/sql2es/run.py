"""
run.py (general.companies.sql2es)
==================================

Pipe curated Companies data from MySQL to Elasticsearch.
"""

from nesta.core.luigihacks.elasticsearchplus import ElasticsearchPlus
from nesta.core.orms.orm_utils import db_session, get_mysql_engine
from nesta.core.orms.orm_utils import object_to_dict
from nesta.core.orms.general_orm import CrunchbaseOrg

from ast import literal_eval
import boto3
import json
import logging
import os


def run():
    test = literal_eval(os.environ["BATCHPAR_test"])
    bucket = os.environ['BATCHPAR_bucket']
    batch_file = os.environ['BATCHPAR_batch_file']
    db_name = os.environ["BATCHPAR_db_name"]
    es_host = os.environ['BATCHPAR_outinfo']
    es_port = int(os.environ['BATCHPAR_out_port'])
    es_index = os.environ['BATCHPAR_out_index']
    es_type = os.environ['BATCHPAR_out_type']
    entity_type = os.environ["BATCHPAR_entity_type"]
    aws_auth_region = os.environ["BATCHPAR_aws_auth_region"]

    # database setup
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db_name)

    # es setup
    es = ElasticsearchPlus(hosts=es_host,
                           port=es_port,
                           aws_auth_region=aws_auth_region,
                           no_commit=("AWSBATCHTEST" in os.environ),
                           entity_type=entity_type,
                           strans_kwargs={'filename': 'companies.json'})

    # Collect input file
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, batch_file)
    org_ids = json.loads(obj.get()['Body']._raw_stream.read())
    org_ids = org_ids[:20 if test else None]
    logging.info(f"{len(org_ids)} organisations retrieved from s3")

    # Pipe orgs to ES
    with db_session(engine) as session:
        query = session.query(CrunchbaseOrg).filter(CrunchbaseOrg.id.in_(org_ids))
        for row in query.all():
            row = object_to_dict(row)
            _row = es.index(index=es_index, doc_type=es_type,
                            id=row.pop('id'), body=row)
    logging.info("Batch job complete.")


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")
    run()
