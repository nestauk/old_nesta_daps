from nesta.core.luigihacks.elasticsearchplus import ElasticsearchPlus
from nesta.packages.novelty.lolvelty import lolvelty
from ast import literal_eval
import os
import boto3
import json
import logging

def run():
    s3_bucket = os.environ["BATCHPAR_bucket"]
    batch_file = os.environ["BATCHPAR_batch_file"]
    count = int(os.environ['BATCHPAR_count'])
    es_index = os.environ['BATCHPAR_index']
    es_host = os.environ['BATCHPAR_outinfo']
    es_port = int(os.environ['BATCHPAR_out_port'])
    es_index = os.environ['BATCHPAR_index']
    es_type = os.environ['BATCHPAR_out_type']
    entity_type = os.environ["BATCHPAR_entity_type"]
    aws_auth_region = os.environ["BATCHPAR_aws_auth_region"]
    fields = literal_eval(os.environ["BATCHPAR_fields"])
    score_field = os.environ["BATCHPAR_score_field"]
    test = literal_eval(os.environ["BATCHPAR_test"])

    # Extract all document ids in this chunk
    s3 = boto3.resource('s3')
    ids_obj = s3.Object(s3_bucket, batch_file)
    logging.info(f'Getting document ids...')
    all_doc_ids = json.loads(ids_obj.get()['Body']._raw_stream.read())
    logging.info(f'Got {len(all_doc_ids)} document ids')
    
    # Set up Elasticsearch
    es = ElasticsearchPlus(hosts=es_host,
                           port=es_port,
                           aws_auth_region=aws_auth_region,
                           no_commit=("AWSBATCHTEST" in os.environ),
                           entity_type=entity_type,
                           do_sort=False)

    min_match = 0.3 if not test else 0.05
    for doc_id in all_doc_ids:
        # Check whether the doc exists with the correct fields
        existing = es.get(es_index, doc_type=es_type,
                          id=doc_id)['_source']
        # Get the score
        score = None
        if any(f in existing for f in fields):
            score = lolvelty(es, es_index, doc_id,
                             fields, total=count,
                             minimum_should_match=min_match)
        # Merge existing info into new doc
        doc = {**existing}
        doc[score_field] = score
        es.index(index=es_index, doc_type=es_type,
                 id=doc_id, body=doc)

if __name__ == "__main__":

    log_level = logging.INFO
    if 'BATCHPAR_outinfo' not in os.environ:
        logging.getLogger('boto3').setLevel(logging.CRITICAL)
        logging.getLogger('botocore').setLevel(logging.CRITICAL)
        logging.getLogger('s3transfer').setLevel(logging.CRITICAL\
)
        logging.getLogger('urllib3').setLevel(logging.CRITICAL)
        log_level = logging.INFO
        pars = {'batch_file': ('ArxivLolveltyTask-2019-08-19'
                               '-True-15662373053397586.json'),
                'config': 'mysqldb.config',
                'bucket': 'nesta-production-intermediate',
                'done': 'False',
                'outinfo': ('https://search-arxlive'
                            '-t2brq66muzxag44zwmrcfrlmq4.'
                            'eu-west-2.es.amazonaws.com'),
                'count': '6000',
                'out_port': '443',
                'fields': "('textBody_abstract_article',)",
                'score_field': "metric_novelty_article",
                'index': 'arxiv_dev',
                'out_type': '_doc',
                'aws_auth_region': 'eu-west-2',
                'entity_type': 'article',
                'test': 'True',
                'routine_id': 'DUMMYTEST'}
        for k, v in pars.items():
            os.environ[f'BATCHPAR_{k}'] = v

    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")    
    run()
