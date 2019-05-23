from ast import literal_eval
from elasticsearch.exceptions import NotFoundError
import logging
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from nesta.production.luigihacks.elasticsearchplus import ElasticsearchPlus
from nesta.packages.health_data.process_mesh import retrieve_mesh_terms
from nesta.packages.health_data.process_mesh import format_mesh_terms
from nesta.packages.health_data.process_mesh import retrieve_duplicate_map
from nesta.packages.health_data.process_mesh import format_duplicate_map
from nesta.production.orms.orm_utils import get_mysql_engine
from nesta.production.orms.orm_utils import load_json_from_pathstub

from nesta.production.orms.nih_orm import Abstracts


def clean_abstract(abstract):
    '''Removes multiple spaces, tabs and newlines.

    Args:
        abstract (str): text to be cleaned

    Returns
        (str): cleaned text
    '''
    abstract = abstract.replace('\t', ' ')
    abstract = abstract.replace('\n', ' ')
    while '  ' in abstract:
        abstract = abstract.replace('  ', ' ')
    abstract.strip()

    return abstract


def run():
    logging.getLogger().setLevel(logging.WARNING)

    bucket = os.environ["BATCHPAR_s3_bucket"]
    abstract_file = os.environ["BATCHPAR_s3_key"]
    dupe_file = os.environ["BATCHPAR_dupe_file"]
    es_config = literal_eval(os.environ["BATCHPAR_outinfo"])
    db = os.environ["BATCHPAR_db"]
    entity_type = os.environ["BATCHPAR_entity_type"]

    # mysql setup
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db)
    Session = sessionmaker(bind=engine)
    session = Session()

    # retrieve a batch of meshed terms
    mesh_terms = retrieve_mesh_terms(bucket, abstract_file)
    mesh_terms = format_mesh_terms(mesh_terms)
    logging.warning(f'batch {abstract_file} contains {len(mesh_terms)} meshed abstracts')

    # retrieve duplicate map
    dupes = retrieve_duplicate_map(bucket, dupe_file)
    dupes = format_duplicate_map(dupes)

    docs = []
    for doc_id, terms in mesh_terms.items():
        try:
            abstract = session.query(Abstracts).filter(Abstracts.application_id == doc_id).one()
        except NoResultFound:
            raise NoResultFound(doc_id)
        clean_abstract_text = clean_abstract(abstract.abstract_text)
        docs.append({'doc_id': doc_id,
                     'mesh_terms': terms,
                     'abstract_text': clean_abstract_text
                     })
        duped_docs = dupes.get(doc_id, [])
        logging.info(f'Found {len(duped_docs)} duplicates')
        for duped_doc in duped_docs:
            docs.append({'doc_id': duped_doc,
                         'mesh_terms': terms,
                         'abstract_text': clean_abstract_text,
                         'duplicate_abstract': True
                         })

    # output to elasticsearch
    field_null_mapping = load_json_from_pathstub("tier_1/field_null_mapping/",
                                                 "health_scanner.json")
    strans_kwargs={'filename':'nih.json',
                   'from_key':'tier_0',
                   'to_key':'tier_1',
                   'ignore':['doc_id']}
    es = Elasticsearch(hosts=es_host,
                           port=es_port,
                           use_ssl=True,
                           entity_type=entity_type,
                           strans_kwargs=strans_kwargs,
                           field_null_mapping=field_null_mapping,
                           null_empty_str=True,
                           coordinates_as_floats=True,
                           country_detection=True,
                           listify_terms=True)

    logging.warning(f'writing {len(docs)} documents to elasticsearch')
    for doc in docs:
        uid = doc.pop("doc_id")
        try:
            existing = es.get(es_config['index'], doc_type=es_config['type'], id=uid)['_source']
        except NotFoundError:
            logging.warning(f"Missing project for abstract: {uid}")
        else:
            doc = {**existing, **doc}
            es.index(index=es_config['index'], 
                     doc_type=es_config['type'], id=uid, body=doc)


if __name__ == '__main__':
    run()
