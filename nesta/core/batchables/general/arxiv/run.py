"""
run.py (general.arxiv)
----------------------

Transfer pre-collected arXiv data from MySQL
to Elasticsearch, whilst labelling arXiv articles
as being EU or not. This differs slightly from
the `arXlive <http://arxlive.org>`_ pipeline, by reflecting the
EURITO project more specificially, and allowing more
in depth analysis of MAG fields of study.
"""

from ast import literal_eval
import boto3
import json
import logging
import os
from nuts_finder import NutsFinder

from nesta.core.luigihacks.elasticsearchplus import ElasticsearchPlus
from nesta.core.luigihacks.luigi_logging import set_log_level
from nesta.core.orms.orm_utils import db_session, get_mysql_engine
from nesta.core.orms.orm_utils import object_to_dict
from nesta.core.orms.arxiv_orm import Article as Art
from nesta.core.orms.grid_orm import Institute as Inst
from nesta.packages.arxiv.deepchange_analysis import is_multinational
from nesta.packages.mag.fos_lookup import build_fos_lookup
from nesta.packages.mag.fos_lookup import make_fos_tree
from nesta.packages.geo_utils.lookup import get_country_region_lookup
from nesta.packages.geo_utils.lookup import get_eu_countries


def generate_grid_lookup(engine):
    class Object(object):
        pass
    country_lookup = get_country_region_lookup()  #<<<ADD CACHE
    grid_lookup = defaultdict(Object)
    with db_session(engine) as session:
        for inst in session.query(Inst).all():
            grid_lookup[inst.id].country = inst.country_code
            grid_lookup[inst.id].region = country_lookup.get(inst.country_code)
            grid_lookup[inst.id].name = inst.name
            grid_lookup[inst.id].latlon = (inst.latitude, inst.longitude)
    return grid_lookup


def flatten_fos(row):
    return [f for fields in row['fields_of_study']['nodes']
            for f in fields if f != []]

def flatten_categories(categories):
    return [cat['description'] for cat in categories]


def calculate_nuts_regions(row, institutes, nuts_finder):
    # Add NUTS regions
    for inst in institutes:
        lat, lon = inst.latlon
        if lat is None or lon is None:
            continue
        nuts = nuts_finder.find(lat=lat, lon=lon)
        for i in range(0, 4):
            name = f'nuts_{i}'
            if name not in row:
                row[name] = set()
            for nut in nuts:
                if nut['LEVL_CODE'] != i:
                    continue
                row[name].add(nut['NUTS_ID'])
    for i in range(0, 4):
        name = f'nuts_{i}'
        if name in row:
            row[name] = list(row[name])
    return row

def generate_authors_and_institutes(mag_authors, good_lookup):
    authors, institutes = None, []
    if mag_authors is not None:
        if all('author_order' in a for a in mag_authors):
            mag_authors = sorted(mag_authors, key=lambda a: a['author_order'])
        gids = [author.get('affiliation_grid_id') for author in mag_authors]
        authors = [author['author_name'].title() for author in mag_authors]
        institutes = [good_lookup[g].name.title() for g in gids if g in good_lookup]
    if institutes == []:
        institutes = [inst.name.title() for inst in good_lookup.values()]
    return authors, institutes


def reformat_row(row, grid_lookup, nuts_finder, inst_matching_threshold=0.9):

    # Create intermediate fields
    mag_authors = row.pop('mag_authors')
    categories = row.pop('categories')
    institutes = row.pop('institutes')
    good_institutes = [inst['institute_id'] for inst in institutes
                       if inst['matching_score'] > inst_matching_threshold]
    good_lookup = {_id: inst for _id, inst in grid_lookup.items() if _id in good_institutes}
    good_institutes = [inst for _id, inst in grid_lookup.items() if _id in good_institutes]
    all_countries = set(inst.country for inst in grid_lookup.values())
    countries = set(inst.country for inst in good_institutes if inst.country is not None)
    regions = set(inst.region for inst in good_institutes if inst.region is not None)
    has_mn = any(is_multinational(inst.name, all_countries)
                 for inst in good_institutes)
    eu_countries = get_eu_countries()  ##<<ADD CACHE
    authors, institutes = generate_authors_and_institutes(mag_authors, good_lookup)

    # Input final fields
    row['year'] = row['created'].year if row['created'] is not None else None
    row['categories'] = [cat['description'] for cat in categories]
    row['fields_of_study'] = make_fos_tree(row['fields_of_study'], fos_lookup)
    row['_fields_of_study'] = flatten_fos(row)
    row['countries'] = list(countries)
    row['regions'] = [region for country, region in regions]
    row['is_eu'] = any(country in eu_countries for country in countries)
    row['has_multinational'] = has_mn
    row = calculate_nuts_regions(row, institutes, nuts_finder)
    row['institutes'] = institutes
    row['authors'] = authors
    return row


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
    fos_lookup = build_fos_lookup(engine, max_lvl=6)
    nf = NutsFinder()

    # es setup
    logging.info('Connecting to ES')
    strans_kwargs = {'filename': 'arxiv.json', 'ignore': ['id']}
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
                           do_sort=False,
                           ngram_fields=['textBody_abstract_article'])

    # collect file
    logging.info('Retrieving article ids')
    nrows = 20 if test else None
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, batch_file)
    art_ids = json.loads(obj.get()['Body']._raw_stream.read())
    logging.info(f"{len(art_ids)} article IDs "
                 "retrieved from s3")

    # Generate lookup tables
    logging.info('Doing country lookup')
    # Generate GRID institute look-up tables
    grid_lookup = generate_grid_lookup(engine)

    # Iterate over articles
    logging.info('Processing rows')
    with db_session(engine) as session:
        for count, obj in enumerate((session.query(Art)
                                     .filter(Art.id.in_(art_ids))
                                     .all())):
            row = object_to_dict(obj)
            row = reformat_row(row, grid_lookup, nf)
            _row = es.index(index=es_index, doc_type=es_type,
                            id=row.pop('id'), body=row)
            if not count % 1000:
                logging.info(f"{count} rows loaded to "
                             "elasticsearch")

    logging.warning("Batch job complete.")


if __name__ == "__main__":
    set_log_level()
    logging.info('Starting...')
    run()
