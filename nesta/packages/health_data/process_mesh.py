import boto3
from collections import defaultdict
import json
import logging
import pandas as pd
import s3fs  # not called but needed to allow pandas to access s3


def retrieve_mesh_terms(bucket, abstract_file):
    """
    Retrieves mesh terms from an s3 bucket.

    Args:
        bucket (str): s3 bucket
        abstract_file (str): path to the abstract file

    Returns:
        (dataframe): whole mesh terms file, with headers appended
    """
    target = f"s3://{bucket}/{abstract_file}"
    logging.debug(f"Retrieving mesh terms from S3: {target}")

    return pd.read_csv(target, sep='|', header=None,
                       names=['doc_id', 'term', 'term_id', 'cui', 'score', 'indices'])


def format_mesh_terms(df):
    """
    Removes unrequired columns and pivots the mesh terms data into a dictionary.

    Args:
        df (dataframe): mesh terms

    Returns:
        (dict): document_id: list of mesh terms
    """
    logging.debug("Formatting mesh terms")
    # remove PRC rows
    df.drop(df[df.term == 'PRC'].index, axis=0, inplace=True)

    # remove invalid error rows
    df.drop(df[df.doc_id.astype(str).str.contains('ERROR.*ERROR', na=False)].index, axis=0, inplace=True)

    # pivot and remove unrequired columns
    doc_terms = {doc_id: list(grouped.term) for doc_id, grouped in df.groupby("doc_id")}
    return doc_terms


def retrieve_duplicate_map(bucket, dupe_file):
    """
    Retrieves the mapping between duplicate abstracts from s3 and processes it.

    Args:
        bucket (str): s3 bucket
        abstract_file (str): path to the duplicate mapping file

    Returns:
        (dict): duplicate doc_id: meshed doc_id
    """
    logging.debug("Retrieving duplicates map from s3")
    s3 = boto3.resource('s3')
    content_object = s3.Object(bucket, dupe_file)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    return json.loads(file_content)


def format_duplicate_map(dupe_map):
    """
    Reformats the duplicate mapping so the meshed abstract is the key.

    Args:
        dupe_map (dict): duplicate doc_id: meshed doc_id

    Returns:
        (dict): meshed doc_id: list of duplicate doc_ids
    """
    logging.debug("Processing duplicates map")
    processed_dupe_map = defaultdict(list)
    for key, value in dupe_map.items():
        processed_dupe_map[value].append(key)

    return processed_dupe_map


if __name__ == '__main__':
    from sqlalchemy.orm import sessionmaker
    from nesta.production.orms.orm_utils import get_mysql_engine
    from nesta.production.orms.nih_orm import Abstracts

    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.DEBUG,
                        # level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")

    # retrieve a batch of meshed terms
    bucket = "innovation-mapping-general"
    abstract_file = "nih_abstracts_processed/mti_nih_abstracts_100065-2683329.txt"
    mesh = retrieve_mesh_terms(bucket, abstract_file)
    mesh = format_mesh_terms(mesh)

    # retrieve duplicate map
    dupe_file = "nih_abstracts/duplicate_mapping/duplicate_mapping.json"
    dupes = retrieve_duplicate_map(bucket, dupe_file)
    dupes = format_duplicate_map(dupes)

    # mysql setup
    engine = get_mysql_engine("MYSQLDB", "mysqldb", "dev")
    Session = sessionmaker(bind=engine)
    session = Session()

    # testing
    one_row = 2061775
    data = session.query(Abstracts).filter(Abstracts.application_id == one_row).one()
    print(data.application_id, data.abstract_text)
    duplicate_projects = dupes[data.application_id]
    print(mesh[one_row])
    print(duplicate_projects)
