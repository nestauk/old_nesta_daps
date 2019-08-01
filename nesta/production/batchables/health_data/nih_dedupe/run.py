import logging
from nesta.production.luigihacks.elasticsearchplus import ElasticsearchPlus
from nesta.production.orms.orm_utils import load_json_from_pathstub

from collections import Counter
import json
import boto3
import os
import numpy as np
import time

def get_value(obj, key):
    try:
        return obj[key]
    except KeyError:
        return

def run():

    # Fetch the input parameters
    s3_bucket = os.environ["BATCHPAR_bucket"]
    batch_file = os.environ["BATCHPAR_batch_file"]
    es_host = os.environ['BATCHPAR_outinfo']
    es_port = int(os.environ['BATCHPAR_out_port'])
    es_new_index = os.environ['BATCHPAR_out_index']
    es_old_index = os.environ['BATCHPAR_in_index']
    es_type = os.environ['BATCHPAR_out_type']
    entity_type = os.environ["BATCHPAR_entity_type"]
    aws_auth_region = os.environ["BATCHPAR_aws_auth_region"]

    # Extract the article ids in this chunk
    s3 = boto3.resource('s3')
    ids_obj = s3.Object(s3_bucket, batch_file)
    art_ids = json.loads(ids_obj.get()['Body']._raw_stream.read())
    logging.info(f'Processing {len(art_ids)} article ids')

    field_null_mapping = load_json_from_pathstub("tier_1/field_null_mappings/",
                                                 "health_scanner.json")
    es = ElasticsearchPlus(hosts=es_host,
                           port=es_port,
                           aws_auth_region=aws_auth_region,
                           no_commit=("AWSBATCHTEST" in os.environ),
                           entity_type=entity_type,
                           field_null_mapping=field_null_mapping,
                           send_get_body_as='POST')

    mlt_query = {"fields": ["textBody_descriptive_project",
                            "title_of_project",
                            "textBody_abstract_project"],
                 "min_term_freq": 1,
                 "max_query_terms": 25,
                 "include": True}

    processed_ids = set()
    for _id in art_ids:
        if _id in processed_ids:
            continue
        # Make the query
        mlt_query['like'] = [{'_index': es_old_index, '_id':_id}]
        body = {"query": {"more_like_this": mlt_query}}
        results = es.search(index=es_old_index, body=body)

        # Mock the result if there are no results
        if results['hits']['total'] == 0:
            _doc = es.get(index=es_old_index,
                          doc_type=es_type,
                          id=_id)
            _doc['_score'] = 1
            results['hits']['max_score'] = 1
            results['hits']['hits'] = [_doc]
        #
        max_score = results['hits']['max_score']
        dupe_ids = {}
        yearly_funds = []
        for hit in results['hits']['hits']:
            src = hit['_source']
            hit_id = hit['_id']
            score = hit['_score']
            # Break when the score is too different
            # (note: the results are sorted by score)
            if np.fabs((score - max_score))/max_score > 0.02:
                break
            #
            year = get_value(src, 'year_fiscal_funding')
            amount = get_value(src, 'cost_total_project')
            start_date = get_value(src, 'date_start_project')
            end_date = get_value(src, 'date_end_project')
            if year is not None:
                yearly_funds += [{'year':year, 'amount': amount,
                                  'start_date': start_date,
                                  'end_date': end_date}]
            dupe_ids[hit_id] = year

        # Ignore if not the final year
        final_id = sorted(dupe_ids.keys())[-1]
        if set(dupe_ids.values()) != {None}:            
            final_id, year = Counter(dupe_ids).most_common()[0]
        body = [hit for hit in results['hits']['hits']
                if hit['_id'] == final_id][0]['_source']
        processed_ids = processed_ids.union(set(dupe_ids))

        # Sort and sum the funding
        yearly_funds = sorted(yearly_funds,
                              key=lambda row: row['year'])
        sum_funding = sum(row['amount'] for row in yearly_funds
                          if row['amount'] is not None)

        # Add funding info and recommit
        body['json_funding_project'] = yearly_funds
        body['cost_total_project'] = sum_funding
        es.index(index=es_new_index,
                 doc_type=es_type,
                 id=final_id,
                 body=body)
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
        pars = {'batch_file': ('2019-05-23-True-'
                               '1564652840333777.json'),
                'config': 'mysqldb.config',
                'bucket': 'nesta-production-intermediate',
                'done': 'False',
                'outinfo': ('https://search-health-scanner-'
                            '5cs7g52446h7qscocqmiky5dn4.'
                            'eu-west-2.es.amazonaws.com'),
                'out_port': '443',
                'out_index': 'nih_v5',
                'in_index': 'nih_v4',
                'out_type': '_doc',
                'aws_auth_region': 'eu-west-2',
                'entity_type': 'paper',
                'test': 'False',
                'routine_id': '2019-05-23-True'}
        for k, v in pars.items():
            os.environ[f'BATCHPAR_{k}'] = v

    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=log_level,
                        format="%(asctime)s:%(levelname)s:%(message)s")
    run()
