from nesta.core.luigihacks.elasticsearchplus import ElasticsearchPlus
from ast import literal_eval
import boto3
import json
import logging
import os
import pandas as pd
import requests
from collections import defaultdict
import numpy as np
from datetime import datetime as dt

from nesta.core.orms.orm_utils import db_session, get_mysql_engine
from nesta.core.orms.orm_utils import load_json_from_pathstub
from nesta.core.orms.orm_utils import object_to_dict
from nesta.core.orms.arxiv_orm import Article
from nesta.core.orms.grid_orm import Institute
from nesta.packages.mag.fos_lookup import build_fos_lookup
from nesta.packages.mag.fos_lookup import split_ids
from nesta.packages.geo_utils.lookup import get_country_region_lookup
from nesta.packages.arxiv.deepchange_analysis import is_multinational
from nesta.packages.nlp_utils.ngrammer import Ngrammer

def hierarchy_field(row_data):
    new_column = []
    all_levels = set()
    count1, count2 = 0, 0
    for value, ancestor in row_data:
        if ancestor not in all_levels:
            new_column.append({'ancestors':[],
                               'value': ancestor,
                               'level': 1})
            all_levels.add(ancestor)
            count1 += 1
        if (value, ancestor) not in all_levels:
            new_column.append({'ancestors':[ancestor],
                               'value': value,
                               'level': 2})
            all_levels.add((value, ancestor))
            count2 += 1
    return new_column, count1, count2


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
    
    # Setup ngrammer
    os.environ['MYSQLDBCONF'] = os.environ['BATCHPAR_config']
    ngrammer = Ngrammer()

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
                           listify_terms=True,
                           do_sort=False)

    # collect file
    nrows = 20 if test else None

    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, batch_file)
    art_ids = json.loads(obj.get()['Body']._raw_stream.read())
    logging.info(f"{len(art_ids)} article IDs "
                 "retrieved from s3")
    
    # Get all grid countries
    # and country: continent lookup
    country_lookup = get_country_region_lookup()                
    with db_session(engine) as session:
        grid_countries = {obj.id: country_lookup[obj.country_code]
                          for obj in session.query(Institute).all()
                          if obj.country_code is not None}
        grid_institutes = {obj.id: obj.name
                           for obj in session.query(Institute).all()}
    #
    current_year = dt.now().year
    with db_session(engine) as session:
        for count, obj in enumerate((session.query(Article)
                                     .filter(Article.id.in_(art_ids))
                                     .all())):
            row = object_to_dict(obj)
            # Extract year from date
            year = 1990
            if row['created'] is not None:
                row['year'] = row['created'].year
                year = row['created'].year

            # Normalise citation count for searchkit
            if row['citation_count'] is None:
                row['citation_count'] = 0
            row['normalised_citation'] = row['citation_count']/np.log(current_year-year+2)

            # If abstract doesn't meet requirements, zero novelty
            # all other novelty will be assigned in a later task
            text = row['abstract'] + ' ' + row['title']
            if (len(text) < 400
                or any(x in row['abstract'].lower()
                       for x in ('withdrawn', 'arxiv administrators'))
                or any(x in row['title'].lower()
                       for x in ('reply to', 'reply on', 
                                 'comment to', 'comment on',
                                 'remarks to', 'remarks on'))):
                row['novelty_of_article'] = 0

            processed_tokens = ngrammer.process_document(row['abstract'])
            row['tokens'] = [t.replace('_', ' ') 
                             for tokens in processed_tokens
                             for t in tokens]

            # Extract field of study Level 0 --> Level 1 paths
            fos = []
            fos_objs = row.pop('fields_of_study')
            fos_ids = set(fos['id'] for fos in fos_objs)
            for f in fos_objs:
                if f['level'] > 0:
                    continue
                fos += [reversed(fos_lookup[(f['id'], cid)])
                        for cid in split_ids(f['child_ids'])
                        if cid in fos_ids]

            # Format hierarchical fields as expected by searchkit
            cats = [(cat['description'], cat['id'].split('.')[0])
                    for cat in row.pop('categories')]
            institutes = row.pop('institutes')
            good_institutes = [i['institute_id'] for i in institutes
                               if i['matching_score'] > 0.9]
            countries = set(grid_countries[inst_id]
                            for inst_id in good_institutes
                            if inst_id in grid_countries)
            row['categories'], _, _ = hierarchy_field(cats)
            row['fos'], _, _ = hierarchy_field(fos)
            row['countries'], _, _ = hierarchy_field(countries)

            # Pull out international institute info
            has_mn = any(is_multinational(inst,
                                          grid_countries.values())
                         for inst in good_institutes)
            row['has_multinational'] = has_mn

            # Generate author & institute properties
            mag_authors = row.pop('mag_authors')
            if mag_authors is None:
                row['authors'] = None
                row['institutes'] = None
                #row['novelty_of_article'] = 0
            else:
                if all('author_order' in a for a in mag_authors):
                    mag_authors = sorted(mag_authors,
                                         key=lambda a: a['author_order'])

                row['authors'] = [author['author_name'].title()
                                  for author in mag_authors]
                if len(row['authors']) > 10:
                    row['authors'] = [f"{row['authors'][0]}, et al"]

                gids = [author['affiliation_grid_id']
                        for author in mag_authors
                        if 'affiliation_grid_id' in author]
                row['institutes'] = [grid_institutes[g].title()
                                     for g in gids
                                     if g in grid_institutes
                                     and g in good_institutes]
                #row['novelty_of_article'] = novelty0 + np.log(novelty1+1)            

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
        from nesta.core.orms.orm_utils import setup_es
        es, es_config = setup_es('dev', True, True,
                                 dataset='arxiv')
        environ = {'batch_file': ('ArxivESTask-2019-09-19-'
                                  'False-1568888970724721.json'),
                   'config': ('/home/ec2-user/nesta-eu/nesta/'
                              'core/config/mysqldb.config'),
                   'db_name': 'dev',
                   'bucket': 'nesta-production-intermediate',
                   'done': "False",
                   'outinfo': ('https://search-arxlive-'
                               't2brq66muzxag44zwmrcfrlmq4.'
                               'eu-west-2.es.amazonaws.com'),
                   'out_port': '443',
                   'out_index': 'arxiv_dev',
                   'out_type': '_doc',
                   'aws_auth_region': 'eu-west-2',
                   'entity_type': 'article',
                   'test': "True"}
        for k, v in environ.items():
            os.environ[f'BATCHPAR_{k}'] = v
    run()
