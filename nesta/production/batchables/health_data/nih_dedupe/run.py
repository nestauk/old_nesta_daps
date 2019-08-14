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
    """Retrieve a value by key if exists, else return None."""
    try:
        return obj[key]
    except KeyError:
        return

def extract_yearly_funds(src):
    """Extract yearly funds"""
    year = get_value(src, 'year_fiscal_funding')
    cost_ref = get_value(src, 'cost_total_project')
    start_date = get_value(src, 'date_start_project')
    end_date = get_value(src, 'date_end_project')
    yearly_funds = []
    if year is not None:
        yearly_funds = [{'year':year, 'cost_ref': cost_ref,
                         'start_date': start_date,
                         'end_date': end_date}]
    return yearly_funds


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
    
    field_null_mapping = load_json_from_pathstub(("tier_1/"
                                                  "field_null_mappings/"),
                                                 "health_scanner.json")
    es = ElasticsearchPlus(hosts=es_host,
                           port=es_port,
                           aws_auth_region=aws_auth_region,
                           no_commit=("AWSBATCHTEST" in os.environ),
                           entity_type=entity_type,
                           field_null_mapping=field_null_mapping,
                           send_get_body_as='POST')

    # Iterate over article IDs
    processed_ids = set()
    for _id in art_ids:
        if _id in processed_ids:  # To avoid duplicated effort
            continue

        # Collect all duplicated data together
        dupe_ids = {}  # For identifying the most recent dupe
        yearly_funds = []  # The new deduped collection of annual funds
        hits = {}
        for hit in es.near_duplicates(index=es_old_index,
                                      doc_id=_id,
                                      doc_type=es_type,
                                      fields=["textBody_descriptive_project",
                                              "title_of_project",
                                              "textBody_abstract_project"]):
            # Extract key values
            src = hit['_source']            
            hit_id = hit['_id']
            # Record this hit
            processed_ids.add(hit_id)
            hits[hit_id] = src
            # Extract year and funding info
            yearly_funds += extract_yearly_funds(src)
            year = get_value(src, 'year_fiscal_funding')
            if year is not None:
                dupe_ids[hit_id] = year

        # Get the most recent instance of the duplicates
        final_id = sorted(hits.keys())[-1]  # default if years are all null
        if len(dupe_ids) > 0:  # implies years are not all null
            final_id, year = Counter(dupe_ids).most_common()[0]
        body = hits[final_id]
        processed_ids = processed_ids.union(set(dupe_ids))

        # Sort and sum the funding
        yearly_funds = sorted(yearly_funds,
                              key=lambda row: row['year'])
        sum_funding = sum(row['cost_ref'] for row in yearly_funds
                          if row['cost_ref'] is not None)

        # Add funding info and commit to the new index
        body['json_funding_project'] = yearly_funds
        body['cost_total_project'] = sum_funding
        body['date_start_project'] = yearly_funds[0]['start_date']  # just in case
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
