from nesta.production.luigihacks.elasticsearchplus import ElasticsearchPlus

from ast import literal_eval
import boto3
import json
import logging
import os
import pandas as pd
import requests
from collections import defaultdict

from nesta.production.orms.orm_utils import db_session, get_mysql_engine
from nesta.production.orms.orm_utils import load_json_from_pathstub
from nesta.production.orms.orm_utils import object_to_dict
from nesta.production.orms.arxiv_orm import Article
from nesta.packages.mag.fos_lookup import build_fos_lookup
from nesta.packages.mag.fos_lookup import split_children

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
    fos_lookup = build_fos_lookup(engine)

    # es setup
    strans_kwargs={'filename':'arxiv.json',
                   'from_key':'tier_0',
                   'to_key':'tier_1',
                   'ignore':['id']}
    es = ElasticsearchPlus(hosts=es_host,
                           port=es_port,
                           aws_auth_region=aws_auth_region,
                           no_commit=("AWSBATCHTEST" in os.environ),
                           entity_type=entity_type,
                           strans_kwargs=strans_kwargs,
                           null_empty_str=True,
                           coordinates_as_floats=True,
                           listify_terms=True)

    # collect file
    nrows = 20 if test else None

    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, batch_file)
    art_ids = json.loads(obj.get()['Body']._raw_stream.read())
    logging.info(f"{len(art_ids)} articles retrieved from s3")

    # 
    with db_session(engine) as session:
        for obj in (session.query(Article)
                    .filter(Article.id.in_(art_ids))
                    .all()):
            row = object_to_dict(obj)
            
            # Extract field of study Level 0 --> Level 1 paths
            fos = []
            fos_objs = row.pop('fields_of_study')
            fos_ids = set(fos['id'] for fos in fos_objs)
            for f in fos_objs:
                if f['level'] > 0:
                    continue
                fos += [fos_lookup[(f['id'], cid)]
                        for cid in split_children(f['child_ids'])
                        if cid in fos_ids]

            # Format as expected by searchkit
            row['fos_level_0'] = [f[0] for f in fos]
            row['fos_level_1'] = [f[1] for f in fos]

            print(row)
            assert False
            
            uid = row.pop('id')
            _row = es.index(index=es_index, doc_type=es_type,
                            id=uid, body=row)
            if not count % 1000:
                logging.info(f"{count} rows loaded to elasticsearch")
    
    logging.warning("Batch job complete.")


if __name__ == "__main__":
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")

    if 'BATCHPAR_outinfo' not in os.environ:
        from nesta.production.orms.orm_utils import setup_es
        es, es_config = setup_es('dev', True, True,
                                 dataset='arxiv')
        
        environ = {"AWSBATCHTEST": "",
                   'BATCHPAR_batch_file': 'crunchbase_to_es-15597291977144725.json', 
                   'BATCHPAR_config': ('/home/ec2-user/nesta/nesta/'
                                       'production/config/mysqldb.config'),
                   'BATCHPAR_db_name': 'production', 
                   'BATCHPAR_bucket': 'nesta-production-intermediate', 
                   'BATCHPAR_done': "False", 
                   'BATCHPAR_outinfo': ('https://search-health-scanner-'
                               '5cs7g52446h7qscocqmiky5dn4.'
                               'eu-west-2.es.amazonaws.com'), 
                   'BATCHPAR_out_port': '443', 
                   'BATCHPAR_out_index': 'companies_v1', 
                   'BATCHPAR_out_type': '_doc', 
                   'BATCHPAR_aws_auth_region': 'eu-west-2', 
                   'BATCHPAR_entity_type': 'company', 
                   'BATCHPAR_test': "False"}

        # environ = {"BATCHPAR_aws_auth_region": "eu-west-2",
        #            "BATCHPAR_outinfo": ("search-health-scanner-"
        #                                 "5cs7g52446h7qscocqmiky5dn4"
        #                                 ".eu-west-2.es.amazonaws.com"),
        #            "BATCHPAR_config":"/home/ec2-user/nesta/nesta/production/config/mysqldb.config",
        #            "BATCHPAR_bucket":"nesta-production-intermediate",
        #            "BATCHPAR_done":"False",
        #            "BATCHPAR_batch_file":"crunchbase_to_es-1559658702669423.json",
        #            "BATCHPAR_out_type": "_doc",
        #            "BATCHPAR_out_port": "443",
        #            "BATCHPAR_test":"True",
        #            "BATCHPAR_db_name":"production",
        #            "BATCHPAR_out_index":"companies_dev",
        #            "BATCHPAR_entity_type":"company"}
        for k, v in environ.items():
            os.environ[k] = v
    run()
