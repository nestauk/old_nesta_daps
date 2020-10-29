"""
run.py (nlp.bert_vectorize)
===========================

Vectorize text documents via BERT
"""

from ast import literal_eval
import boto3
import json
import logging
import os

from nesta.core.orms.orm_utils import db_session, get_mysql_engine
from nesta.core.orms.orm_utils import insert_data, object_to_dict
from nesta.core.orms.orm_utils import get_class_by_tablename
from nesta.core.orms.orm_utils import get_base_from_orm_name

from sentence_transformers import SentenceTransformer


def run():
    # Pull out job parameters
    test = literal_eval(os.environ["BATCHPAR_test"])
    bucket = os.environ['BATCHPAR_bucket']
    batch_file = os.environ['BATCHPAR_batch_file']
    db_name = os.environ["BATCHPAR_db_name"]
    in_module = os.environ["BATCHPAR_in_class_module"]
    in_tablename = os.environ["BATCHPAR_in_class_tablename"]
    out_module = os.environ["BATCHPAR_out_class_module"]
    out_tablename = os.environ["BATCHPAR_out_class_tablename"]
    id_field = os.environ["BATCHPAR_id_field_name"]
    text_field = os.environ["BATCHPAR_text_field_name"]
    bert_model_name = os.environ.get("BATCHPAR_bert_model", "distilbert-base-nli-stsb-mean-tokens")

    # Instantiate SentenceTransformer
    model = SentenceTransformer(bert_model_name)

    # Database setup
    engine = get_mysql_engine("BATCHPAR_config", "mysqldb", db_name)

    # Retrieve list of object ids from S3
    nrows = 20 if test else None
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, batch_file)
    _ids = json.loads(obj.get()['Body']._raw_stream.read())
    logging.info(f"{len(_ids)} objects retrieved from s3")

    # Retrieve each document by id
    _class = get_class_by_tablename(in_module, in_tablename)
    id_attribute = getattr(_class, id_field)
    text_attribute = getattr(_class, text_field)
    with db_session(engine) as session: 
        query = (session 
                 .query(id_attribute, text_attribute)
                 .filter(id_attribute.in_(_ids)) 
                 .limit(nrows)) 
        docs, ids = [], []            
        for _id, text in query.all(): 
            if text is None:
                continue
            docs.append(text) 
            ids.append(_id)

    # Convert text to vectors
    embeddings = model.encode(docs)
    # Write to output
    out_data = [{id_field: _id, "vector": ["%.5f" % v for v in embedding.tolist()]}
                for _id, embedding in zip(ids, embeddings)]
    Base = get_base_from_orm_name(out_module)
    out_class = get_class_by_tablename(out_module, out_tablename)
    insert_data("BATCHPAR_config", "mysqldb", db_name, Base,
                out_class, out_data, low_memory=True)


if __name__ == "__main__":
    if 'BATCHPAR_config' not in os.environ:
        os.environ["BATCHPAR_id_field_name"] = "application_id"
        os.environ["BATCHPAR_config"] = "/home/ec2-user/nesta/nesta/core/config/mysqldb.config"
        os.environ["BATCHPAR_bucket"] = "nesta-production-intermediate"
        os.environ["BATCHPAR_out_class_tablename"] = "nih_phr_vectors"
        os.environ["BATCHPAR_batch_file"] = "RootTask-2020-10-23-True-16034673506743276.json"
        os.environ["BATCHPAR_in_class_module"] = "nih_orm"
        os.environ["BATCHPAR_routine_id"] = "RootTask-2020-10-23-True"
        os.environ["BATCHPAR_out_class_module"] = "nih_orm"
        os.environ["BATCHPAR_test"] = "True"
        os.environ["BATCHPAR_db_name"] = "dev"
        os.environ["BATCHPAR_text_field_name"] = "phr"
        os.environ["BATCHPAR_in_class_tablename"] = "nih_projects"
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")
    run()
