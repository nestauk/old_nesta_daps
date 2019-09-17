from ast import literal_eval
import boto3
import json
import logging
import os

from nesta.core.luigihacks.elasticsearchplus import ElasticsearchPlus
from nesta.core.luigihacks.luigi_logging import set_log_level
from nesta.core.orms.orm_utils import db_session, get_mysql_engine
from nesta.core.orms.orm_utils import load_json_from_pathstub
from nesta.core.orms.orm_utils import object_to_dict
from nesta.core.orms.arxiv_orm import Article as Art
from nesta.core.orms.grid_orm import Institute as Inst
from nesta.packages.arxiv.deepchange_analysis import is_multinational
from nesta.packages.mag.fos_lookup import build_fos_lookup
from nesta.packages.mag.fos_lookup import split_ids
from nesta.packages.geo_utils.lookup import get_country_region_lookup

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
    logging.info('Retrieving engine connection')
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", 
                              db_name)
    logging.info('Building FOS lookup')
    max_lvl = 4  # used again later
    fos_lookup = build_fos_lookup(engine, max_lvl=max_lvl)

    # es setup
    logging.info('Connecting to ES')
    strans_kwargs={'filename':'eurito/arxiv-eu.json',
                   'from_key':'tier_0', 'to_key':'tier_1',
                   'ignore':['id']}
    es = ElasticsearchPlus(hosts=es_host,
                           port=es_port,
                           aws_auth_region=aws_auth_region,
                           no_commit=("AWSBATCHTEST" in 
                                      os.environ),
                           entity_type=entity_type,
                           strans_kwargs=strans_kwargs,
                           null_empty_str=True,
                           coordinates_as_floats=True,
                           listify_terms=True,
                           do_sort=False)

    # collect file
    logging.info('Retrieving article ids')
    nrows = 20 if test else None
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, batch_file)
    art_ids = json.loads(obj.get()['Body']._raw_stream.read())
    logging.info(f"{len(art_ids)} article IDs "
                 "retrieved from s3")
    
    # Get all grid countries
    # and country: continent lookup
    logging.info('Doing country lookup')
    country_lookup = get_country_region_lookup()                
    with db_session(engine) as session:
        grid_countries = {obj.id: country_lookup[obj.country_code]
                          for obj in session.query(Inst).all()
                          if obj.country_code is not None}
        grid_institutes = {obj.id: obj.name
                           for obj in session.query(Inst).all()}
    #
    logging.info('Processing rows')
    with db_session(engine) as session:
        for count, obj in enumerate((session.query(Art)
                                     .filter(Art.id.in_(art_ids))
                                     .all())):
            row = object_to_dict(obj)
            # Extract year from date
            if row['created'] is not None:
                row['year'] = row['created'].year

            # Normalise citation count for searchkit
            if row['citation_count'] is None:
                row['citation_count'] = 0

            # Extract field of study
            fos_objs = row.pop('fields_of_study')
            fos_ids = set(fos['id'] for fos in fos_objs)
            for f in fos_objs:
                if f["level"] > max_lvl:
                    continue
                key0 = f'fos_level_{f["level"]}'
                if key0 not in row:
                    row[key0] = []
                for cid in split_ids(f['child_ids']):
                    if cid not in fos_ids:
                        continue                    
                    key1 = f'fos_level_{f["level"]+1}'
                    if key1 not in row:
                        row[key1] = []
                    try:
                        fos = fos_lookup[(f['id'], cid)]
                    except KeyError:
                        continue
                    row[key0].append(fos[0])
                    row[key1].append(fos[1])
            for k, v in row.items():
                if not (type(v) is str 
                        and v.startswith('fos_level')):
                    continue
                row[k] = list(set(v))

            # Format hierarchical fields as expected by searchkit
            row['categories'] = [cat['description'] 
                                 for cat in row.pop('categories')]
            institutes = row.pop('institutes')
            good_institutes = [i['institute_id'] 
                               for i in institutes
                               if i['matching_score'] > 0.9]
            countries = set(grid_countries[inst_id]
                            for inst_id in good_institutes
                            if inst_id in grid_countries)
            row['countries'] = [c for c, r in countries]
            row['regions'] = [r for c, r in countries]

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
            else:
                if all('author_order' in a for a in mag_authors):
                    mag_authors = sorted(mag_authors,
                                         key=lambda a: 
                                         a['author_order'])
                row['authors'] = [author['author_name'].title()
                                  for author in mag_authors]
                gids = [author['affiliation_grid_id']
                        for author in mag_authors
                        if 'affiliation_grid_id' in author]
                row['institutes'] = [grid_institutes[g].title()
                                     for g in gids
                                     if g in grid_institutes
                                     and g in good_institutes]
            uid = row.pop('id')
            _row = es.index(index=es_index, doc_type=es_type,
                            id=uid, body=row)
            if not count % 1000:
                logging.info(f"{count} rows loaded to "
                             "elasticsearch")

    logging.warning("Batch job complete.")


if __name__ == "__main__":
    set_log_level()
    if 'BATCHPAR_outinfo' not in os.environ:
        set_log_level(True)
        from nesta.core.orms.orm_utils import setup_es
        es, es_config = setup_es('dev', True, True,
                                 dataset='arxiv-eu')
        environ = {'outinfo':('https://search-eurito-dev-'
                              'vq22tw6otqjpdh47u75bh2g7ba.'
                              'eu-west-2.es.amazonaws.com'),
                   'config':('/home/ec2-user/nesta-eu/'
                             'nesta/core/config/mysqldb.config'),
                   'bucket':'nesta-production-intermediate',
                   'batch_file': ('ElasticsearchTask-2019-'
                                  '09-17-True-'
                                  '15687118121284823.json'),
                   'out_port': '443',
                   'out_type': '_doc',
                   'db_name': 'dev',
                   'entity_type': 'article',
                   'out_index': 'arxiv_dev',
                   'test': 'True',
                   'aws_auth_region': 'eu-west-2'}
        for k, v in environ.items():
            os.environ[f'BATCHPAR_{k}'] = v
    logging.info('Starting...')
    run()
