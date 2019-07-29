import logging
from nesta.production.luigihacks.elasticsearchplus import ElasticsearchPlus
from nesta.production.orms.orm_utils import load_json_from_pathstub

from collections import Counter
import json
import boto3
import os
import numpy as np
import time

def run():

    # Fetch the input parameters
    s3_bucket = os.environ["BATCHPAR_bucket"]
    batch_file = os.environ["BATCHPAR_batch_file"]
    es_host = os.environ['BATCHPAR_outinfo']
    es_port = int(os.environ['BATCHPAR_out_port'])
    es_index = os.environ['BATCHPAR_out_index']
    es_type = os.environ['BATCHPAR_out_type']
    entity_type = os.environ["BATCHPAR_entity_type"]
    aws_auth_region = os.environ["BATCHPAR_aws_auth_region"]

    # # Extract the article ids in this chunk
    # s3 = boto3.resource('s3')
    # ids_obj = s3.Object(s3_bucket, batch_file)
    # art_ids = set(json.loads(ids_obj.get()['Body']._raw_stream.read()))
    # logging.info(f'Processing {len(art_ids)} article ids')

    field_null_mapping = load_json_from_pathstub("tier_1/field_null_mappings/",
                                                 "health_scanner.json")
    strans_kwargs={'filename':'nih.json',
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
                           send_get_body_as='POST')

    results = es.search(index=es_index, body={"size": 1000,
                                              "query" : { 
                                                  "match_all" : {} 
                                              },
                                              "stored_fields": []
                                          })
    
    art_ids = [hit['_id'] for hit in results['hits']['hits']]
    assert art_ids[0] == '9197629'

    mlt = {"fields": ["textBody_descriptive_project",
                      "title_of_project"],
           "min_term_freq" : 1,
           "max_query_terms" : 25,
           "include": True}
    
    processed_ids = set()
    for _id in art_ids:
        if _id in processed_ids:
            continue
        # Make the query
        mlt['like'] = [{'_index': es_index, '_id':_id}]
        body = {"query": {"more_like_this" : mlt}}
        results = es.search(index=es_index, body=body)

        # Ignore if this index has been removed
        if results['hits']['total'] == 0:
            assert False
            continue
        # 
        max_score = results['hits']['max_score']
        dupe_ids = {}
        yearly_funds = []
        for hit in results['hits']['hits']:
            src = hit['_source']            
            hit_id = hit['_id']
            score = hit['_score']
            if np.fabs((score - max_score))/max_score > 0.02:
                continue
            #
            year = src['year_fiscal_funding']
            amount = src['cost_total_project']
            yearly_funds += [{'year':year, 'amount': amount}]
            dupe_ids[hit_id] = year

        # Ignore if not the final year
        final_id, year = Counter(dupe_ids).most_common()[0]
        body = [hit for hit in results['hits']['hits']
                if hit['_id'] == final_id][0]['_source']
        processed_ids = processed_ids.union(set(dupe_ids))
        dupe_ids.pop(final_id)
        yearly_funds = sorted(yearly_funds,
                              key=lambda row: row['year'])
        sum_funding = sum(row['amount'] for row in yearly_funds
                          if row['amount'] is not None)
        # Remove dupes by id
        #print(dupe_ids.keys())
        # Add yearly funds
        #print(yearly_funds)
        # Add total funding
        #print(sum_funding)
        #if len(processed_ids) > 100:
        #    break

    logging.info(f'Processed {len(processed_ids)} ids')
    logging.info("Batch job complete.")

# For local debugging
if __name__ == "__main__":

    log_level = logging.INFO
    if 'BATCHPAR_outinfo' not in os.environ:
        logging.getLogger('boto3').setLevel(logging.CRITICAL)
        logging.getLogger('botocore').setLevel(logging.CRITICAL)
        logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
        logging.getLogger('urllib3').setLevel(logging.CRITICAL)
        log_level = logging.INFO
        pars = {"batch_file":("nih_abstracts_processed/22-07-2019/"
                              "nih_abstracts_9638504-9926622.out.txt"),
                "outinfo": ('https://search-health-scanner'
                            '-5cs7g52446h7qscocqmiky5dn4.eu-west'
                            '-2.es.amazonaws.com'),
                "out_port": '443',
                "out_index": 'nih_v4',
                'out_type': '_doc',
                'aws_auth_region': 'eu-west-2',
                "bucket":"innovation-mapping-general",
                "entity_type":"paper"}
        for k, v in pars.items():
            os.environ[f'BATCHPAR_{k}'] = v

    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=log_level,
                        format="%(asctime)s:%(levelname)s:%(message)s")
    run()
