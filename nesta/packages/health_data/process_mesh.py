import boto3
import json
import pandas as pd
import s3fs  # needs to be imported to allow pandas to access s3


def retrieve_mesh(bucket, abstract_file):
    """
    Retrieves mesh terms from an s3 bucket.

    Args:
        bucket (str): s3 bucket
        abstract_file (str): path to the abstract file

    Returns:
        (dataframe): whole mesh terms file, with headers appended
    """
    target = f"s3://{bucket}/{abstract_file}"

    df = pd.read_csv(target, sep='|', header=None,
                     names=['doc_id', 'term', 'term_id', 'cui', 'score', 'indices'])
    return df


def format_mesh_terms(df):
    """
    Removes unrequired columns and pivots the mesh terms data into a dictionary.

    Args:
        df (dataframe): mesh terms

    Returns:
        (dict): document_id: list of mesh terms
    """
    # remove PRC rows
    df.drop(df[df.term == 'PRC'].index, axis=0, inplace=True)

    # pivot and remove unrequired columns
    doc_terms = {doc_id: list(grouped.term) for doc_id, grouped in df.groupby("doc_id")}
    return doc_terms


def retrieve_duplicate_map():
    """
    Retrieves a json file with the mapping between duplicate abstracts.

    Returns:
        (dict): duplicate doc_id: closest doc_id sent for meshing
    """
    bucket = "innovation-mapping-general"
    dupe_file = "nih_abstracts/duplicate_mapping/duplicate_mapping.json"

    s3 = boto3.resource('s3')

    content_object = s3.Object(bucket, dupe_file)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content


if __name__ == '__main__':
    bucket = "innovation-mapping-general"
    abstract_file = "nih_abstracts_processed/mti_nih_abstracts_100065-2683329.txt"
    mesh = retrieve_mesh(bucket, abstract_file)
    mesh = format_mesh_terms(mesh)

    dupes = retrieve_duplicate_map()
